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
print(volume_names)


def manage_volumes():
    '''
    Manage Volume and Renaming based on their ages
    '''
    with ThreadPoolExecutor() as executor:
        #Rename Previous month's Volume to 2 months back
        
        if prev_volume in volume_names:
            executor.submit(
                spark.sql, f"ALTER VOLUME {env}.gold.{prev_volume} RENAME TO {env}.gold.{prev_volume_2}"
            )
        #renaming present Volume in main volume to previous month. So a current main volume can be created for the current month
        if main_volume in volume_names:
            executor.submit(
                spark.sql, f"ALTER VOLUME {env}.gold.{main_volume} RENAME TO {env}.gold.{prev_volume}"
            )
    #create main volume if not exists of the current month
    if main_volume not in get_volumes():
        spark.sql(f"CREATE VOLUME IF NOT EXISTS {env}.gold.nightlyfiles")
 
# if prev_volume in volume_names:
#     sql_spark= f"""ALTER VOLUME {env}.gold.{prev_volume} RENAME TO {env}.gold.{prev_volume_2}"""
#     spark.sql(sql_spark)    

# if main_volume in volume_names:
#     sql_spark= f"""ALTER VOLUME  {env}.gold.{main_volume} RENAME TO {env}.gold.{prev_volume}"""
#     spark.sql(sql_spark)

# #create main volume if not exists of the current month
# if main_volume not in get_volumes():
#     spark.sql(f"CREATE VOLUME IF NOT EXISTS {env}.gold.nightlyfiles")


 def get_enabled_views(control_table):
  '''
  Fetch enabled views from the control table
  '''
  enabled_views = spark.table(control_table).filter("enabled = true").select ("sourceTable").rdd.flatMap(lambda x: x).collect()
  return enabled_views


def get_view_sizes(view_names, source_table):
  '''
  Fetch the row count for each view for optimized processing
  '''
  #Return Dictionares mapping views to row counts
  view_sizes = {}
  for view_names in view_names:
    row_count = spark.table(f"{source_table}.{view_names}").count()
    view_sizes[view_names] = row_count
  return view_sizes # views based on row count


def generate_file_path(view_names):
  '''
  Generate file backup file paths for for each enabled views
  '''
  #return list of backup file paths
  return[
    f"{backup_path}/{view.split('.')[-1]}/{view.split('.')[-1]}_{yearmonth}.txt"
    for view in view_names
  ]

def create_backup_file(file_paths):
    '''
    Create backup files for each enabled view and saving them to the backup directory
    '''
    #Creating backup directories for each view
    backup_dirs = set("/".join(fp.replace(".txt", "_backup.txt").split("/")[:-1]) for fp in file_paths)
    for backup_dir in backup_dirs:
        dbutils.fs.mkdirs(backup_dir)

    #Function to move backup files to their repective backup directory
    def backup_file(file_path):
        backup_path = file_path.replace(".txt", "_backup.txt")
        dbutils.fs.mv(file_path, backup_path, recurse=True)
        print(f"Backup created at: {backup_path}")

    #Executing the backup function in parallel
    with ThreadPoolExecutor(max_workers=16) as executor:
        executor.map(backup_file, file_paths)


def fetch_vendor_data(view_names, base_path, source_table,view_sizes):
    '''
    fetch and process data in parellel. Smaller views are processed first followed by larger views wich are equal to 10000000 or larger
    '''
    dbutils.fs.mkdirs(base_path)

    def process_view(view_name):
        '''
        Process a single view by reading, repartitioning and saving the data to csv
        '''
        #read data from the delta table
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
    smaller_views = [view for view in view_names if view_sizes.get(view,0) < 10000000]
    larger_views = [view for view in view_names if view_sizes.get(view,0) >= 10000000]

    # Process smaller views in parallel
    print("\n")
    print("Processing smaller views in parallel")
    print("\n")
    
    with ThreadPoolExecutor(max_workers=16) as executor:
        executor.map(process_view, smaller_views)

    # Process larger views sequentially
    print("\n")
    print("Processing larger views")
    print("\n")
    # with ThreadPoolExecutor(max_workers=16) as executor:
    #     executor.map(process_view, larger_views)
    print("Processing larger views sequentially")
    for view_name in larger_views:
        process_view(view_name)

  

