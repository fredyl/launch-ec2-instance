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
      .option("delimeter", "|") \
      .save(file_path_csv) 
    dbutils.fs.mv(file_path_csv, file_path_txt, recurse=True)
    print(f"Saved data to {file_path_txt}")

fetch_vendor_data(view_names, base_path, source_table)
