data_list = get_billing_data()
data_list = get_holman_endpoints_data()
#Upsert the data to the table
if data_list:
    Holman_Upsert_data(data_type, data_key,data_list, primary_key)
    print(f"Data upserted for data type {data_type} with code value {code_value}")
else:
    print(f"No data fetched")

dbutils.fs.rm(d_path, True) #deleting the directory after processing the data


def get_billing_data():
    data_type = "billing"
    data_key = "billing"
    primary_key = "vehicleNumber"
    code_key = "billingTypeCode"
    key_code = "invoiceDateCode"
    data_list = []

    d_path = f'/Volumes/{env}/bronze_vendor/holman/{data_type}'
    dbutils.fs.mkdirs(d_path)

    for code_value in range(1, 4):
        for key_value in range(1, 4):
            print(f"\nChecking data for {data_type} with code key {code_key} = {code_value} and {key_code}={key_value}")

            # Retrieve total pages and skip if no pages are available
            total_pages =  get_total_pages(data_type, code_key, code_value, key_code, key_value)
            if total_pages == 0:
                print(f"No pages available for {data_type} with code key {code_key} = {code_value}. Skipping...")
                continue
            print(f"Total Pages : {total_pages}")

            paginated_urls, num_batches = generate_paginated_urls(data_type,data_key, code_key, code_value,total_pages,key_value, key_code)
            print(f"Total URLs : {len(paginated_urls)}")

            batch_df = spark.createDataFrame(paginated_urls)
            batch_df = batch_df.withColumn("part", floor(col("pageNumber") / 100)).repartition(num_batches, "part")
            result_df = batch_df.withColumn("data", fetch_data_udf(col("url"), col("pageNumber"), lit(data_type), lit(code_value), lit(d_path),lit(key_value))).cache()

            #collect all file paths from result_df and convert to json then dataframe
            file_path = [row.data for row in result_df.select("data").collect()]
            if not file_path:
                print(f"No Files found for {data_type} with code key {code_key} = {code_value} and key code {key_code}={key_value}.Skipping....")
                continue

            o_df = spark.read.json(file_path)
            if o_df.rdd.isEmpty():
                print(f"No data found for {data_type} with code key {code_key} = {code_value} and key code {key_code} = {key_value}")

            json_data = o_df.select(data_key).toJSON().collect()
            cleaned_data  = [replace_null_values(json.loads(row)) for row in json_data]
            o_df = spark.createDataFrame([Row(**item) for item in cleaned_data])

            #fetching the data from the json dataframe o_df and converting to list
            data_list = []
            for row in o_df.collect():
                if data_key in row and isinstance(row[data_key], list):
                    data_list.extend(row[data_key])
            
            print(f"Total data fetched for {data_type} with code key {code_key} = {code_value} and key code {key_code} = {key_value} is {len(data_list)} ")
            print(f"Total records accumulated is {len(data_list)} ")

    return data_list



    def get_endpoints_data():
    '''
    Main execution loop for using udf_based parallel execution to fetch processing data
    '''

    # list of dictionaries where each dictionary contains the data type, data key, primary key for an endpoint
    holman_coded_endpoints = [
        {"data_type": "maintenance", "code_key": "billPaidDateCode", "data_key": "maintenance", "primary_key": "clientVehicleNumber"},
        {"data_type": "violation", "code_key": "violationDateCode", "data_key": "violations", "primary_key": "record_id"},
        {"data_type": "billing", "code_key": "billingTypeCode", "data_key": "billing", "primary_key": "vehicleNumber"},
        {"data_type": "fuels", "code_key": "transDateCode", "data_key": "us", "primary_key": "usRecordID"},
    ]



    # Main execution loop for fetching and processing data
    for endpoint_config in holman_coded_endpoints:
        data_type = endpoint_config["data_type"]
        code_key = endpoint_config["code_key"]
        data_key = endpoint_config["data_key"]
        primary_key = endpoint_config["primary_key"]

        # Define the directory path to store the fetched data and create directory is it does not exists

        endpoint_options = update_endpoints(data_type)
        if endpoint_options is None:
            print(f"Skipping {data_type} endpoint is not defined")
            continue

        # code_values = [endpoint_options.get("code_value", i) for i in range(1,4) if "code_value" not in endpoint_options]
        if "code_value" in endpoint_options:
            code_values = [endpoint_options["code_value"]]
        else:
            code_values = [1,2,3]

        url_ext = endpoint_options.get("url_ext", "")

        d_path = f'/Volumes/{env}/bronze_vendor/holman/{data_type}'
        dbutils.fs.mkdirs(d_path)

        for code_value in code_values:  # Looping through code_key values 1 to 3

            print(f"\nChecking data for {data_type} with code key {code_key} = {code_value}")

            # Retrieve total pages and skip if no pages are available
            total_pages = get_total_pages(data_type, code_key, code_value)
            print(f"Total Pages : {total_pages}")
            
            # Skip if no pages are available
            if total_pages == 0:
                print(f"No pages available for {data_type} with code key {code_key} = {code_value}. Skipping...")
                continue
            
            #Generating pagination urls, creating url datafarmes and batching the urls. 
            #Add part columnto partition the pages into groups of 100
            #repartition the df based on num_batches and cache the df for faster processing
            #use udf to fetch the data from the url

            paginated_urls, num_batches = generate_paginated_urls(data_type,data_key, code_key, code_value,total_pages)

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


        #fetching the data from the json dataframe o_df and converting to list
        data_list = []
        for row in o_df.collect():
            if data_key in row and isinstance(row[data_key], list):
                data_list.extend(row[data_key])

    return data_list
