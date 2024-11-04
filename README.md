
holman_coded_endpoints = [
   
    # {"data_type": "fuels", "code_key": "transDateCode", "data_key": "can", "primary_key": "clientVehicleNumber"},
   
    {"data_type": "violation", "code_key": "violationDateCode", "data_key": "violations", "primary_key": "record_id"},
    # {"data_type": "billing", "code_key": "billingTypeCode", "data_key": "billing", "primary_key": "vehicleNumber"},
    #  {"data_type": "fuels", "code_key": "transDateCode", "data_key": "us", "primary_key": "usRecordID"},
]

# Main execution loop for fetching and processing data
for endpoint_config in holman_coded_endpoints:
    data_type = endpoint_config["data_type"]
    code_key = endpoint_config["code_key"]
    data_key = endpoint_config["data_key"]
    primary_key = endpoint_config["primary_key"]


    # for code_value in range(1, 4):  # Looping through code_key values 1 to 3
    code_value = 2
    print(f"\nChecking data for {data_type} with code key {code_key} = {code_value}")

    # Retrieve total pages and skip if no pages are available
    total_pages = get_total_pages(data_type, code_key, code_value)
    print(f"Total Pages : {total_pages}")
    
    if total_pages == 0:
        print(f"No pages available for {data_type} with code key {code_key} = {code_value}. Skipping...")
        continue
    
    paginated_urls, num_batches = generate_paginated_urls(data_type,data_key, code_key, code_value,total_pages)
    print(f"Total URLs : {len(paginated_urls)}")
    batch_df = spark.createDataFrame(paginated_urls)
    batch_df = batch_df.withColumn("part", floor(col("pageNumber") / 100)).repartition(num_batches, "part")
    # result_df = batch_df.withColumn("data", fetch_data_udf(col("url"), col("pageNumber"))).cache()
    result_df = batch_df.withColumn("data", fetch_data_udf(col("url"), col("pageNumber"), lit(data_type), lit(code_value))).cache()
    # display(result_df)
    
    batch_df = spark.read.json(f"{chk_path}")
    file_path = [row.data for row in result_df.select("data").collect()]
    print(len(file_path))
    output_df = spark.read.json(file_path)


    data_list = []
    for row in output_df.collect():
         if data_key in row and isinstance(row[data_key], list):
             data_list.extend([record.asDict() for record in row[data_key]])
            #  print(json.dumps(data_list, indent=2  ))
        
    if data_list:
    # cleaned_data = replace_null_values(data_list)
        cleaned_data = replace_null_values(data_list)
        # print(json.dumps(cleaned_data, indent=2  ))
        Holman_Upsert_data(data_type, data_key,cleaned_data, primary_key)
    #     print(f"Data upserted for data type {data_type} with code value {code_value}")

    for f in dbutils.fs.ls(chk_path):
        dbutils.fs.rm(f.path, True)
        







[UNABLE_TO_INFER_SCHEMA] Unable to infer schema for JSON. It must be specified manually. SQLSTATE: 42KD9
File <command-2638019235405315>, line 38
     35 result_df = batch_df.withColumn("data", fetch_data_udf(col("url"), col("pageNumber"), lit(data_type), lit(code_value))).cache()
     36 # display(result_df)
---> 38 batch_df = spark.read.json(f"{chk_path}")
     39 file_path = [row.data for row in result_df.select("data").collect()]
     40 print(len(file_path))
File /databricks/spark/python/pyspark/errors/exceptions/captured.py:261, in capture_sql_exception.<locals>.deco(*a, **kw)
    257 converted = convert_exception(e.java_exception)
    258 if not isinstance(converted, UnknownException):
    259     # Hide where the exception came from that shows a non-Pythonic
    260     # JVM exception message.
--> 261     raise converted from None
    262 else:
    263     raise
