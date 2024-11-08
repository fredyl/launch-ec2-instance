def get_holman_api_response(token, endpoint, retries=3):
    #getting an API response from the HOLMAN API endpoint and retry if token expired
    base_url = "https://customer-experience-api.arifleet.com/v1/"
    base_urlD = "https://customer-experience-api.arifleet.com/v1/delta"
    url = base_url + endpoint
    if use_delta:
        url = base_urlD + endpoint
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f"Bearer {token}"
    }
    for attempts in range(retries):
        response = requests.get(url, headers=headers)
        if response.status_code == 204:
            print(f"Received 204, No Content from the API.")
            return response, None
        elif response.status_code == 200:
            return response.json()
        elif response.status_code == 401:
            print(f"Token expired, refreshing...")
            token = get_token()
            time.sleep(5)
        else:
            raise Exception("Failed:", response.status_code, response.text)
    raise Exception("Max retries reached for token refresh")


def get_total_pages(data_type, code_key, code_value, use_delta = False):
    #calculating total number of pages for the given data type and endpoint
    endpoint = f"{data_type}?{code_key}={code_value}"
    response_data  = get_holman_api_response(token, endpoint)
    if response_data:
        return int(response_data.get("totalPages", 1))
    return 0
    

def generate_paginated_urls(data_type,data_key, code_key, code_value,total_pages,use_delta = False,batch_size=200):
    # Generates paginated URLs based on the total number of pages  and calculating the number of batches used for parrallel processing

    base_url = "https://customer-experience-api.arifleet.com/v1/"
    if use_delta:
        base_url = "https://customer-experience-api.arifleet.com/v1/delta/"
    pagination_urls = []
    num_batches = (total_pages // batch_size) + 1
    print(f" num_batches : {num_batches}")

    #getting urls for each page and appending them to pagination_urls
    for i in range(num_batches):
        for page in range(i * batch_size + 1, min((i + 1) * batch_size + 1, total_pages +1)):
            url = f"{base_url}{data_type}?{code_key}={code_value}&pageNumber={page}"
            pagination_urls.append({"pageNumber": page, "url": url})
    print('len', len(pagination_urls))
    return pagination_urls, num_batches





def get_holman_data(token, data_type, data_key,use_delta = False ):
    #Define the endpoint and get the response and oing pagination
    
    limit=200
    page=1
    data_list = []
    table_name = f"bronze.holman_{data_type}"
    use_delta = spark.catalog.tableExists(table_name)
    

    #Pagination logic
    while True:
        endpoint = f"{data_type}?pageNumber={page}"
        response_data = get_holman_data(token, data_type=data_type, data_key=data_key, use_delta=False)
        batch_data = response_data.get(data_key, [])
        if len(batch_data) == 0:
            print(f"No more records on page {page}, Stopping Pagination")
            break
        data_list.extend(batch_data)
        print(f"Processing page{page}, with {len(batch_data)} records")
        page +=1

    print(f"endpoint:{endpoint}")
    print(len(data_list))
    
    return data_list


'''
define a set of endpoints with corresponding data_type and data_keys for Holman API without code_keys
'''
holman_endpoints = {
    "vehicles": ("inventory", "clientVehicleNumber"),
    "maintenance": ("maintenance", "clientVehicleNumber"),
    "accidents": ("accident", "clientVehicleNumber"),
    "odometer": ("odometerHistory", "vin"),
    "persons": ("person", "contactEmployeeId"),
}


#iterate through the endpoints to fetch and upsert data
for data_type, (data_key, primary_key) in holman_endpoints.items():
    data_list = get_holman_data(token, data_type=data_type, data_key=data_key, use_delta=False)
    data_list = replace_null_values(data_list)

    if data_list:
        Holman_Upsert_data(data_type, data_key, data_list, primary_key)
    else:
        print(f"No data found for {data_type}_{data_key}")
print("All data fetched")




'''
define a set of endpoint for corresponding data keys, creating a merge key used to match rows between the source df and delta table
'''

data_type="orders" 
data_key="orderHistory"
primary_key="clientVehicleNumber"
merge_keys= ["clientVehicleNumber", "upfitPoIssueDate","tg_updated"]  
table_name = f"bronze.holman_{data_type}"
print(table_name)


#retrieve  data from get_holman_data
data_list = get_holman_data(token, data_type =data_type, data_key=data_key,use_delta = False)
df = spark.createDataFrame(data_list)
current_time = current_timestamp()
df = df.withColumn("tg_inserted", current_time).withColumn("tg_updated", current_time)

#doing an upsert while using merge key in merge condition.
#if table exists do an upsert and if table does not exist creating a new table
if spark.catalog.tableExists(table_name):
    print(f"Table {table_name} exists. Performing upsert (merge)...")
    delta_table = DeltaTable.forName(spark, table_name)
    merge_condition = " AND " .join([f"new_data.{primary_key} = existing_data.{primary_key}" for primary_key in merge_keys])
    delta_table.alias("existing_data") \
        .merge(
            df.alias("new_data"), expr(merge_condition)
        ) \
        .whenMatchedUpdate(
            set={
                **{col_name: col(f"new_data.{col_name}") for col_name in df.columns if col_name != primary_key and col_name != "tg_inserted"}
            }
        ) \
        .whenNotMatchedInsertAll() \
        .execute()
    print(f"Upsert completed for {table_name}")
else:
    print(f"Table does not exist, creating new table {table_name}")
    df.write.format("delta").saveAsTable(table_name) #creating delta table if table does not exist


'''
Main execution loop for using udf_based parallel execution to fetch processing data
'''

# list of dictionaries where each dictionary contains the data type, data key, primary key for an endpoint
holman_coded_endpoints = [
    {"data_type": "violation", "code_key": "violationDateCode", "data_key": "violations", "primary_key": "record_id"},
    {"data_type": "billing", "code_key": "billingTypeCode", "data_key": "billing", "primary_key": "vehicleNumber"},
     {"data_type": "fuels", "code_key": "transDateCode", "data_key": "us", "primary_key": "usRecordID"},
]


table_name = f"bronze.holman_{data_type}"
use_delta = spark.catalog.tableExists(table_name)
print(f"Using Delta Table: {use_delta}")

# Main execution loop for fetching and processing data
for endpoint_config in holman_coded_endpoints:
    data_type = endpoint_config["data_type"]
    code_key = endpoint_config["code_key"]
    data_key = endpoint_config["data_key"]
    primary_key = endpoint_config["primary_key"]

    # Define the directory path to store the fetched data and create directory is it does not exists
    d_path = f'/Volumes/{env}/bronze_vendor/holman/{data_type}'
    dbutils.fs.mkdirs(d_path)


    for code_value in range(1, 4):  # Looping through code_key values 1 to 3
        print(f"\nChecking data for {data_type} with code key {code_key} = {code_value}")

        # Retrieve total pages and skip if no pages are available
        total_pages = get_total_pages(data_type, code_key, code_value,use_delta)
        print(f"Total Pages : {total_pages}")
        
        # Skip if no pages are available
        if total_pages == 0:
            print(f"No pages available for {data_type} with code key {code_key} = {code_value}. Skipping...")
            continue
        
        #Generating pagination urls, creating url datafarmes and batching the urls. 
        #Add part columnto partition the pages into groups of 100
        #repartition the df based on num_batches and cache the df for faster processing
        #use udf to fetch the data from the url

        paginated_urls, num_batches = generate_paginated_urls(data_type,data_key, code_key, code_value,total_pages,use_delta)
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

        #Upsert the data to the table
        Holman_Upsert_data(data_type, data_key,data_list, primary_key)
        print(f"Data upserted for data type {data_type} with code value {code_value}")

dbutils.fs.rm(d_path, True) #deleting the directory after processing the data
print("Data processing completed")
