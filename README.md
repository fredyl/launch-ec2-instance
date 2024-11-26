






def generate_file_path(view_names):
  file_paths= []
  for view in view_names:
    view_names = view.split(".")[-1]
    file_path = f"{backup_path}/{view_names}/{view_names}_{yearmonth}.csv"
    file_paths.append(file_path)
  return file_paths
file_paths = generate_file_path(view_names)


def create_backUp_file(file_paths):
    for file_path in file_paths:
        backup_path = file_path.replace(".csv", "_backup.csv")
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
    

def get_enabled_views(control_table):
  '''
  Fetch enabled views from the control table
  '''
  enabled_views = spark.table(control_table).filter("enabled = true").select ("sourceTable").rdd.flatMap(lambda x: x).collect()
  return enabled_views
enabled_views = get_enabled_views(control_table)



def fetch_vendor_data(view_name, base_path, source_table): 
  '''
  Loop through the views and read data from the views and add a column yearmonth to the dataframe
  '''
  # enabled_views = get_enabled_views(control_table)
  dbutils.fs.mkdirs(base_path)

  for view in view_name:
    view_name = view.split(".")[-1]
    data_df = spark.read.format("delta").table(f"{source_table}.{view_name}")
    file_path = f"{base_path}/{view_name}/{view_name}_{yearmonth}.csv"
    data_df.write.mode("overwrite").format("csv") \
      .option("header", "true") \
      .save(file_path) 
    print(f"Saved data to {file_path}")

fetch_vendor_data(view_name, base_path, source_table)
