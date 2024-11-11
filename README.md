def get_billing_data(token):
    data_type = "billing"
    data_key = "billing"
    primary_key = "vehicleNumber"
    code_key = "billingTypeCode"
    key_code = "invoiceDateCode"
    data_list = []  # Defined outside the loop to accumulate data across all pages

    # Define storage path for billing data
    d_path = f'/Volumes/{env}/bronze_vendor/holman/{data_type}'
    dbutils.fs.mkdirs(d_path)

    for code_value in range(1, 4):  # Loop through billingTypeCode values 1 to 3
        for key_value in range(1, 4):  # Loop through invoiceDateCode values 1 to 3
            print(f"\nProcessing {data_type} with {code_key}={code_value} and {key_code}={key_value}")

            # Retrieve total pages for this combination
            total_pages = get_total_pages(data_type, code_key, code_value, key_code, key_value)
            if total_pages == 0:
                print(f"No pages available for {data_type} with {code_key}={code_value} and {key_code}={key_value}. Skipping...")
                continue
            print(f"Total Pages: {total_pages}")

            # Generate paginated URLs
            paginated_urls, num_batches = generate_paginated_urls(data_type, data_key, code_key, code_value, total_pages, key_value, key_code)
            print(f"Total URLs generated: {len(paginated_urls)}")

            # Create Spark DataFrame for URLs and repartition for performance
            batch_df = spark.createDataFrame(paginated_urls)
            batch_df = batch_df.withColumn("part", floor(col("pageNumber") / 100)).repartition(num_batches, "part")
            
            # Fetch data from URLs using UDF
            result_df = batch_df.withColumn("data", fetch_data_udf(col("url"), col("pageNumber"), lit(data_type), lit(code_value), lit(d_path))).cache()

            # Collect file paths and convert to JSON DataFrame
            file_paths = [row.data for row in result_df.select("data").collect()]
            if not file_paths:
                print(f"No files found for {data_type} with {code_key}={code_value} and {key_code}={key_value}. Skipping...")
                continue
            
            o_df = spark.read.json(file_paths)
            if o_df.rdd.isEmpty():
                print(f"No data in JSON files for {data_type} with {code_key}={code_value} and {key_code}={key_value}. Skipping...")
                continue

            # Debug message to show data collected per page
            print(f"Number of records in JSON files for {data_type} with {code_key}={code_value} and {key_code}={key_value}: {o_df.count()}")

            # Clean data and create a structured DataFrame
            json_data = o_df.select(data_key).toJSON().collect()
            cleaned_data = [replace_null_values(json.loads(row)) for row in json_data]
            o_df = spark.createDataFrame([Row(**item) for item in cleaned_data])

            # Extend data_list with fetched data
            for row in o_df.collect():
                if data_key in row and isinstance(row[data_key], list):
                    data_list.extend(row[data_key])

    # Debug message for total accumulated data before upsert
    print(f"Total records accumulated for upsert: {len(data_list)}")

    # Upsert all data to the table after processing all combinations
    if data_list:
        Holman_Upsert_data(data_type, data_key, data_list, primary_key)
        print(f"Data upserted for {data_type} with all code combinations.")
    else:
        print(f"No data was fetched for {data_type}.")

    # Clean up: delete the directory after processing the data
    dbutils.fs.rm(d_path, True)
