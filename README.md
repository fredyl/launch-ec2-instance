for endpoint_config in holman_coded_endpoints:
    data_type = endpoint_config["data_type"]
    code_key = endpoint_config["code_key"]
    data_key = endpoint_config["data_key"]
    primary_key = endpoint_config["primary_key"]

    for code_value in range(1, 4):  # Looping through code_key values 1 to 3
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
        display(result_df)
        
        batch_df = spark.read.json(f"{chk_path}")
        file_path = [row.data for row in result_df.select("data").collect()]
        print(len(file_path))
        output_df = spark.read.json(file_path)
        display(output_df)
        cleaned_data =replace_nulls_in_dataframe(output_df)
        display(cleaned_data)
        #partition data and fect using udf
        
        

        #collect and process data
        data_list = []
        for row in cleaned_data.collect():
             if data_key in row and isinstance(row[data_key], str) and not row[data_key].startswith("Error") and not row[data_key].startswith("Exception"):
                try:
                    parsed_data = json.loads(row(data_key))
                    if isinstance(parsed_data, list):
                        parsed_data = parsed_data
                    else:
                        parsed_data = []
                    data_list.extend(parsed_data)
                    
                    print(json.dumps(data_list,indent=2))
                except json.JSONDecodeError as e:
                    print(f"Error parsing JSON for URL: {row.url}: {e}")

        # Upsert data if available
        if data_list:
            # cleaned_data = replace_null_values(data_list)
            Holman_Upsert_data(data_type, data_key, primary_key)
            print(f"Data upserted for data type {data_type} with code value {code_value}")
    for f in dbutils.fs.ls(chk_path):
        dbutils.fs.rm(f.chk_path, True)
