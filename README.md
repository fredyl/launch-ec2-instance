yearmonth = datetime.now().strftime("%Y%m")
base_path = f'/Volumes/dev/bronze_vendor/nightlyfiles'

sql_spark= f"CREATE VOLUME IF NOT EXISTS {env}.bronze_vendor.nightlyfiles"
spark.sql(sql_spark)


def get_enabled_views(control_table):
  '''
  Fetch enabled views from the control table
  '''
  enabled_views = spark.table(control_table).filter("enabled = true").select ("sourceTable").rdd.flatMap(lambda x: x).collect()
  return enabled_views



def fetch_vendor_data(control_table, base_path): 
  '''
  Loop through the views and read data from the views and add a column yearmonth to the dataframe
  '''

  
  enabled_views = get_enabled_views(control_table)

  dbutils.fs.mkdirs(base_path)

  for view in enabled_views:
    view_name = view.split(".")[-1]
    data_df = spark.read.format("delta").table(f"prod.gold_vendor_nightly.{view_name}")
    file_path = f"{base_path}/{view_name}/{view_name}_{yearmonth}.csv"
    data_df.write.mode("overwrite").format("csv") \
      .option("header", "true") \
      .save(file_path) 
    print(f"Saved data to {file_path}")



