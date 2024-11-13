def get_billing_data():
    data_type = "billing"
    data_key = "billing"
    primary_key = "vehicleNumber"
    code_key = "billingTypeCode"
    key_code = "invoiceDateCode"
    data_list = []
    
    endpoint_options = update_endpoints(data_type)
    if endpoint_options is None:
        print(f"Skipping {data_type} endpoint is not defined")
        return
    
    url_ext = endpoint_options.get("url_ext", "")

    d_path = f'/Volumes/{env}/bronze_vendor/holman/{data_type}'
    dbutils.fs.mkdirs(d_path)

    for code_value in range(1, 4):
        for key_value in range(1, 4):
        # key_value = 3
            print(f"\nChecking data for {data_type} with code key {code_key} = {code_value} and {key_code}={key_value}")

            # Retrieve total pages and skip if no pages are available
            total_pages =  get_total_pages(token, data_type, code_key, code_value, key_code, key_value)
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

            #Upsert the data to the table
            
            Holman_Upsert_data(data_type, data_key,data_list,key_value, primary_key)
            print(f"Data upserted for data type {data_type} with code value {code_value}")
            

    dbutils.fs.rm(d_path, True) #deleting the directory after processing the data



get_billing_data()
      


def update_endpoints(data_type):
    '''
    endpoint to use in case data type delta table exists
    '''
    table_name = f"bronze.holman_{data_type}"
    if spark.catalog.tableExists(table_name):
        if data_type in ["maintenance","violation", "fuels"]:
            return {"code_value": 1}
        elif data_type in ["vehicles", "accidents", "odometer"]:
            return {"delta_url": "delta"}
        elif data_type in ["persons", "orders"]:
            return {"run_all": True}
        elif data_type == "billing":
            return {"key_code": 1}
        else:
            #If table does not exists and data type is given allow data to be loaded, so table will be craated at the upsert level
            return {"run_all": True}


def get_total_pages(data_type, code_key, code_value, key_code, key_value):
    #calculating total number of pages for the given data type and endpoint
    if data_type == "billing":
        endpoint = f"{data_type}?{code_key}={code_value}&{key_code}={key_value}"
    else:
        endpoint = f"{data_type}?{code_key}={code_value}"
    response_data  = get_holman_api_response(token, endpoint)
    if response_data:
        return int(response_data.get("totalPages", 1))
    return 0

# Generates paginated URLs based on the total number of pages  and calculating the number of batches used for parrallel processing
def generate_paginated_urls(data_type,data_key, code_key, code_value,total_pages,key_value, key_code, batch_size=200):
    base_url = "https://customer-experience-api.arifleet.com/v1/"
    pagination_urls = []
    num_batches = (total_pages // batch_size) + 1
    print(f" num_batches : {num_batches}")

    #getting urls for each page and appending them to pagination_urls
    for i in range(num_batches):
        for page in range(i * batch_size + 1, min((i + 1) * batch_size + 1, total_pages +1)):
            url = f"{base_url}{data_type}?{code_key}={code_value}&pageNumber={page}"
            if data_type == "billing" and key_code is not None :
                url += f"&{key_code}={key_value}"
            pagination_urls.append({"pageNumber": page, "url": url})
    print('len', len(pagination_urls))
    return pagination_urls, num_batches


  
    
      
