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

    print(f"Saved data to {file_path}")
