def get_billing_data():
    data_type = "billing"
    data_key = "billing"
    primary_key = "vehicleNumber"
    code_key = "billingTypeCode"
    key_code = "invoiceDateCode"
    data_list = []
    
    # https://customer-experience-api.arifleet.com/v1/billing?billingTypeCode=1&invoiceDateCode=3

    d_path = f'/Volumes/{env}/bronze_vendor/holman/{data_type}'
    dbutils.fs.mkdirs(d_path)

    for code_value in range(1, 4):
        for key_value in range(1, 4):
            d_path = f'/Volumes/{env}/bronze_vendor/holman/{data_type}'
            dbutils.fs.mkdirs(d_path)
            print(f"\nChecking data for {data_type} with code key {code_key} = {code_value}")
            # Retrieve total pages and skip if no pages are available
            total_pages =  get_total_pages(data_type, code_key, code_value, key_code, key_value)
            if total_pages == 0:
                print(f"No pages available for {data_type} with code key {code_key} = {code_value}. Skipping...")
                continue
            print(f"Total Pages : {total_pages}")

            paginated_urls, num_batches = generate_paginated_urls(data_type,data_key, code_key, code_value,total_pages,key_value, key_code)
            print(f"Total URLs : {len(paginated_urls)}")

            print(f"Total URLs : {len(paginated_urls)}")
            batch_df = spark.createDataFrame(paginated_urls)
            batch_df = batch_df.withColumn("part", floor(col("pageNumber") / 100)).repartition(num_batches, "part")
            result_df = batch_df.withColumn("data", fetch_data_udf(col("url"), col("pageNumber"), lit(data_type), lit(code_value), lit(d_path))).cache()

            #collect all file paths from result_df and convert to json then dataframe
            file_path = [row.data for row in result_df.select("data").collect()]
            o_df = spark.read.json(file_path)
            json_data = o_df.select(data_key).toJSON().collect()
            cleaned_data  = [replace_null_values(json.loads(row)) for row in json_data]
            o_df = spark.createDataFrame([Row(**item) for item in cleaned_data])
            display(o_df)

            #fetching the data from the json dataframe o_df and converting to list
            data_list = []
            for row in o_df.collect():
                if data_key in row and isinstance(row[data_key], list):
                    data_list.extend(row[data_key])

            #Upsert the data to the table
            Holman_Upsert_data(data_type, data_key,data_list, primary_key)
            print(f"Data upserted for data type {data_type} with code value {code_value}")

    dbutils.fs.rm(d_path, True) #deleting the directory after processing the data
