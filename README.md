from pyspark.sql.functions import year,col,month,dayofmonth
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta
from concurrent.futures import ThreadPoolExecutor

control_table = "prod.admin.vendornightlyfiles"
source_table = "prod.gold_vendor_nightly"
yearmonth = datetime.now().strftime("%Y%m")
prev_month = (datetime.now() - relativedelta(months=1)).strftime("%Y%m")
prev_month_2 = (datetime.now() - relativedelta(months=2)).strftime("%Y%m")
backup_path = f'''/Volumes/dev/gold/nightlyfiles_{prev_month}_backup'''
base_path = f'/Volumes/dev/gold/nightlyfiles'

prev_volume = f"nightlyfiles_{prev_month}_backup"
prev_volume_2 = f"nightlyfiles_{prev_month_2}_backup"
main_volume ="nightlyfiles"
start_time = datetime.now()
print(f"start time is : {start_time}")

# Get volume names
def get_volumes():
    return [row["volume_name"] for row in spark.sql(f"SHOW VOLUMES IN {env}.gold").select("volume_name").collect()]
volume_names = get_volumes()


# Manage volumes in parallel
def manage_volumes():
    volume_names = get_volumes()
    with ThreadPoolExecutor() as executor:
        if prev_volume in volume_names:
            executor.submit(
                spark.sql, f"ALTER VOLUME {env}.gold.{prev_volume} RENAME TO {env}.gold.{prev_volume_2}"
            )
 
        volume_names = get_volumes()
        if prev_volume_2 in volume_names:
            executor.submit(
                spark.sql, f"DROP VOLUME IF EXISTS {env}.gold.{prev_volume_2}"
            )


        if main_volume in volume_names:
            executor.submit(
                spark.sql, f"ALTER VOLUME {env}.gold.{main_volume} RENAME TO {env}.gold.{prev_volume}"
            )
    volume_names = get_volumes()
    if main_volume not in volume_names:
        spark.sql(f"CREATE VOLUME IF NOT EXISTS {env}.gold.nightlyfiles")
 
def get_enabled_views(control_table):
  '''
  Fetch enabled views from the control table
  '''
  enabled_views = spark.table(control_table).filter("enabled = true").select ("sourceTable").rdd.flatMap(lambda x: x).collect()
  return enabled_views


def get_view_sizes(enabled_views, source_table):
  view_sizes = {}
  for view in enabled_views:
    row_count = spark.table(f"{source_table}.{view}").count()
    view_sizes[view] = row_count
  return view_sizes # views based on row count

def generate_file_path(enabled_views):
  return[
    f"{backup_path}/{view.split('.')[-1]}/{view.split('.')[-1]}_{yearmonth}.txt"
    for view in enabled_views
  ]

# Create backup files in parallel
def create_backup_file(file_paths):
    backup_dirs = set("/".join(fp.replace(".txt", "_backup.txt").split("/")[:-1]) for fp in file_paths)
    for backup_dir in backup_dirs:
        dbutils.fs.mkdirs(backup_dir)

    def backup_file(file_path):
        backup_path = file_path.replace(".txt", "_backup.txt")
        dbutils.fs.mv(file_path, backup_path, recurse=True)
        print(f"Backup created at: {backup_path}")

    with ThreadPoolExecutor(max_workers=16) as executor:
        executor.map(backup_file, file_paths)


# Fetch vendor data in parallel
def fetch_vendor_data(enabled_views, base_path, source_table,view_sizes):
    '''
    fetch and process data in parellel. Smaller views are processed first followed by larger views wich are equal to 100000000 or larger
    '''
    dbutils.fs.mkdirs(base_path)

    def process_view(view):
        '''
        Process a single view by reading, repartitioning and saving the data to csv
        '''
        #read data from the delta table
        view_name = view.split(".")[-1]
        data_df = spark.read.format("delta").table(f"{source_table}.{view_name}")
        data_df = data_df.select([data_df[col].cast("string").alias(col) for col in data_df.columns])

        #save data to csv
        file_path_txt = f"{base_path}/{view_name}/{view_name}_{yearmonth}.txt"
        data_df.repartition(50).write.mode("overwrite").format("csv") \
            .option("header", "true") \
            .option("delimiter", "|") \
            .save(file_path_txt)
        print(f"Saved data to {file_path_txt}")

    #splitting the views into smaller and larger views based on view sizes
    smaller_views = [view for view in enabled_views if view_sizes[view] < 1000000]
    larger_views = [view for view in enabled_views if view_sizes[view] >= 1000000]

    # Process smaller views in parallel
    print("Processing smaller views in parallel")
    with ThreadPoolExecutor(max_workers=16) as executor:
        executor.map(process_view, smaller_views)

    # Process larger views sequentially
    print("Processing larger views")
    with ThreadPoolExecutor(max_workers=16) as executor:
        executor.map(process_view, larger_views)


  # Main execution
manage_volumes()

enabled_views = get_enabled_views(control_table)
file_paths = generate_file_path(enabled_views)

if main_volume in volume_names:
    create_backup_file(file_paths)
else:
    print("No backup required")

#Getting the views
view_sizes = get_view_sizes(enabled_views, source_table)

#Processing the views
fetch_vendor_data(enabled_views, base_path, source_table,view_sizes)

#Deleting the volumes that are 2 months older than the present date
volume_names = get_volumes()
if prev_volume_2 in volume_names:
    sql_spark= f"DROP VOLUME IF EXISTS {env}.gold.{prev_volume_2}"
    spark.sql(sql_spark)

#Calculating the time taken to complete the process
end_time = datetime.now()
elapsed_time = end_time - start_time
print(f"Total time taken: {elapsed_time}"
      )
