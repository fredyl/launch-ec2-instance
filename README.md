

control_table = "prod.admin.vendornightlyfiles"
source_table = "prod.gold_vendor_nightly"
yearmonth = datetime.now().strftime("%Y%m")
prev_month = (datetime.now() - relativedelta(months=1)).strftime("%Y%m")
prev_month_2 = (datetime.now() - relativedelta(months=2)).strftime("%Y%m")
backup_path = f'''/Volumes/dev/bronze_vendor/nightlyfiles_{prev_month}_backup'''
base_path = f'/Volumes/dev/bronze_vendor/nightlyfiles'

prev_volume = f"nightlyfiles_{prev_month}_backup"
prev_volume_2 = f"nightlyfiles_{prev_month_2}_backup"
main_volume ="nightlyfiles"

def get_volumes():
  volumes = spark.sql(f"SHOW VOLUMES IN {env}.bronze_vendor").collect()
  return [volume.volume_name for volume in volumes]
volume_names = get_volumes()


if prev_volume_2 in volume_names:
    sql_spark= f"DROP VOLUME IF EXISTS {env}.bronze_vendor.{prev_volume_2}"
    spark.sql(sql_spark)

if prev_volume in volume_names:
    sql_spark= f"""ALTER VOLUME {env}.bronze_vendor.{prev_volume} RENAME TO {env}.bronze_vendor.{prev_volume_2}"""
    spark.sql(sql_spark)

if main_volume in volume_names:
    sql_spark= f"""ALTER VOLUME  {env}.bronze_vendor.{main_volume} RENAME TO {env}.bronze_vendor.{prev_volume}"""
    spark.sql(sql_spark)

volume_names = get_volumes()
if main_volume not in volume_names:
    sql_spark= f"CREATE VOLUME IF NOT EXISTS {env}.bronze_vendor.nightlyfiles"
    spark.sql(sql_spark)


def get_enabled_views(control_table):
  '''
  Fetch enabled views from the control table
  '''
  enabled_views = spark.table(control_table).filter("enabled = true").select ("sourceTable").rdd.flatMap(lambda x: x).collect()
  return enabled_views
enabled_views = get_enabled_views(control_table)


def generate_file_path(view_names):
  file_paths= []
  for view in view_names:
    view_names = view.split(".")[-1]
    file_path = f"{backup_path}/{view_names}/{view_names}_{yearmonth}.txt"
    file_paths.append(file_path)
  return file_paths
file_paths = generate_file_path(view_names)


def create_backUp_file(file_paths):
    for file_path in file_paths:
        backup_path = file_path.replace(".txt", "_backup.txt")
        # Ensure the backup directory exists
        backup_dir = "/".join(backup_path.split("/")[:-1])
        dbutils.fs.mkdirs(backup_dir)
        # Copy the file to the backup location
        dbutils.fs.cp(file_path, backup_path, recurse=True)
        print(f"Backup path created: {backup_path}")
# Example usage
if  main_volume in volume_names:
    create_backUp_file(file_paths)
else:
    print("No backup required")




    
def fetch_vendor_data(view_names, base_path, source_table): 
  '''
  Loop through the views and read data from the views and add a column yearmonth to the dataframe
  '''
  # enabled_views = get_enabled_views(control_table)
  dbutils.fs.mkdirs(base_path)

  for view in view_names:
    view_name = view.split(".")[-1]
    data_df = spark.read.format("delta").table(f"{source_table}.{view_name}")
    for column in data_df.columns:
      data_df = data_df.withColumn(column, data_df[column].cast("string"))
    file_path_csv = f"{base_path}/{view_name}/{view_name}_{yearmonth}.csv"
    file_path_txt = file_path_csv.replace(".csv", ".txt")
    data_df.write.mode("overwrite").format("csv") \
      .option("header", "true") \
      .option("delimiter", "|") \
      .save(file_path_csv) 
    dbutils.fs.mv(file_path_csv, file_path_txt, recurse=True)
    print(f"Saved data to {file_path_txt}")

fetch_vendor_data(view_names, base_path, source_table)
