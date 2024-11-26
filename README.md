








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
