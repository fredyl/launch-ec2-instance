def fetch_vendor_data(enabled_views,control_table): 
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
  return allView_data_df
  
