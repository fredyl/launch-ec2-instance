


def get_enabled_views(control_table):
  '''
  Fetch enabled views from the control table
  '''
  enabled_views = spark.table(control_table).filter("enabled = true").select ("sourceTable").collect()
  return enabled_views
enabled_views = get_enabled_views(control_table)



def fetch_vendor_data(control_table): 
  '''
  Loop through the views and read data from the views and add a column yearmonth to the dataframe
  '''

  allView_data_df = {}
  enabled_views = get_enabled_views(control_table)
  # enabled_views = spark.table(control_table).filter("enabled = true").select("sourceTable").collect()
  
  for view in enabled_views:
      view_name = view.sourceTable.split(".")[-1]
      data_df = spark.read.format("delta").table(f"prod.gold_vendor_nightly.{view_name}")
      data_df = data_df.withColumn("YearMonth", (data_df.XXDATE.cast("string").substr(1,6)))
      allView_data_df[view_name] = data_df
      # data_df.show(5)
  
  return allView_data_df

data_df = fetch_vendor_data(control_table)


def save_partition_full_loads(view_name,df,base_path):
  '''
  The function will save full loads files grouped by YearMonth with optimized Partitioning
  '''
  
  YearMonth =df.select("YearMonth").distinct().rdd.flatMap(lambda x: x).collect()
  for yearmonth in YearMonth:
    file_path = f"{base_path}/{view_name}/{view_name}_{yearmonth}.csv"

    df.write.mode("overwrite").format("csv") \
      .partitionBy("YearMonth") \
      .option("header", "true") \
      .save(file_path) 

    print(f"Saved data to {file_path



    def process_saved_views(control_table):
  
    dbutils.fs.mkdirs("/Volumes/dev/bronze_vendor/nightlyfiles")
    base_path = '/Volumes/dev/bronze_vendor/nightlyfiles'
                 
    
    dbutils.fs.mkdirs(base_path)

    view_data = fetch_vendor_data(control_table)
    for view_name, df in view_data.items():
      print(f"Processing {view_name}")
      save_partition_full_loads(view_name,df,base_path)
