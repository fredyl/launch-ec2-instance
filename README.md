def fetch_vendor_data(enabled_views): 
  # enabled_views = spark.table(control_table).filter("enabled = true").select("sourceTable").collect()
  for view in enabled_views:
    view_name = view.sourceTable
  data_df = spark.read.format("delta").table(f"prod.{view_name}")
  data_df = data_df.withColumn("YearMonth", (data_df.XXDATE.cast("int").substr(1,6)))
  display(data_df)
