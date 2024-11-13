def get_holman_api_response(token, endpoint, retries=3, use_delta = False ):
    #getting an API response from the HOLMAN API endpoint and retry if token expired
    base_url = "https://customer-experience-api.arifleet.com/v1/"
    url = base_url + endpoint
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
            return {"key_value": 1}
        else:
            #If table does not exists and data type is given allow data to be loaded, so table will be craated at the upsert level
            return {"run_all": True}

def get_total_pages(data_type, code_key, code_value, key_code=None, key_value=None):
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
def generate_paginated_urls(data_type,data_key, code_key, code_value,total_pages,key_value=None, key_code=None, batch_size=200):
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




def fetch_data_from_url(url, page, data_type, code_value, d_path,key_value=None):
    '''
    Fetch data from given urland writing to file in file path in json format''
    '''
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f"Bearer {token}"
    }
    response = requests.get(url, headers=headers)
    if data_type == "billing":
        filePath = f'{d_path}/{data_type}_{code_value}_{key_value}_{page}.json'
    else:
        filePath = f'{d_path}/{data_type}_{code_value}_{page}.json' # file path
    if response.status_code != 200:
        return f"Error: {response.status_code}"

    #writting response to file
    with open(filePath, 'w') as f:
        f.write(response.text)
    
    return filePath


fetch_data_udf = udf(fetch_data_from_url, StringType())


def replace_null_values(data_key):
    #Replacing Null Values with empty string
    if isinstance (data_key, dict):
        return {k: replace_null_values(v) if v is not None else "" for k, v in data_key.items()}
    elif isinstance (data_key, list):
        return [replace_null_values(item) if item is not None else "" for item in data_key]
    else:
        return data_key if data_key is not None else ""


def get_holman_data(token, data_type, data_key,url_ext=None):
    #Define the endpoint and get the response and oing pagination
    
    base_endpoint = f"{data_type}/{url_ext}" if url_ext else data_type
    limit=200
    page=1
    data_list = []

    #Pagination logic
    while True:
        endpoint = f"{base_endpoint}?pageNumber={page}"
        response_data = get_holman_api_response(token, endpoint)
        batch_data = response_data.get(data_key, []) if response_data else []
        if len(batch_data) == 0:
            print(f"No more records on page {page}, Stopping Pagination")
            break
        data_list.extend(batch_data)
        print(f"Processing page{page}, with {len(batch_data)} records")
        page +=1

    print(len(data_list))
    
    return data_list


def fetch_data_upsert(result_df, data_key):
    #collect all file paths from result_df and convert to json then dataframe
        file_path = [row.data for row in result_df.select("data").collect()]
        if not file_path:
            print("No file found ... Skipping")
            return[]
        
        o_df = spark.read.json(file_path)
        if o_df.rdd.isEmpty():
            print(f"No data found in json")
            return[]

        json_data = o_df.select(data_key).toJSON().collect()
        cleaned_data  = [replace_null_values(json.loads(row)) for row in json_data]
        o_df = spark.createDataFrame([Row(**item) for item in cleaned_data])


        #fetching the data from the json dataframe o_df and converting to list
        data_list = []
        for row in o_df.collect():
            if data_key in row and isinstance(row[data_key], list):
                data_list.extend(row[data_key])
        print(f"Total records accumulated is {len(data_list)} ")
        return data_list

def Holman_Upsert_data(data_type, data_list, primary_key= None):
    '''
    Getting dictionary to map data_type used to create a unique key to be used as primary key for data that seems similar
    and then do an Upsert
    '''
    table_name = f"bronze.holman_{data_type}"
    print(f"table_name : {table_name}")
    df = spark.createDataFrame(data_list)
    current_time = current_timestamp()
    df = df.withColumn("tg_inserted", current_time).withColumn("tg_updated", current_time)

    #Dictionary to map data_type defined in key_col and columns concatenated
    key_col = {
        'vehicles': 'deliveryDate',
        'maintenance': 'record_id',
        'accidents': 'record_id',
        'odometer': 'record_id',
        'billing': 'record_id',
    }

    # Check if the data_type is in the dictionary key_col and concatenate columns if it is
    if data_type in key_col:
        combined_key = "combined_key"
        df = df.withColumn(combined_key, concat(col(primary_key), lit("_"), col(key_col[data_type])))
        primary_key = combined_key
    
    #drop duplicate records for persons data_type
    if data_type == "persons":
        df = df.dropDuplicates([primary_key])

    #Upsert logic
    if spark.catalog.tableExists(table_name):
        print(f"Table {table_name} exists. Performing upsert (merge)...")
        delta_table = DeltaTable.forName(spark, table_name)
        delta_table.alias("existing_data")\
            .merge(df.alias("new_data"), expr(f"new_data.{primary_key} = existing_data.{primary_key}")) \
            .whenMatchedUpdate( set={col_name: col(f"new_data.{col_name}") for col_name in df.columns if col_name not in [primary_key, "tg_inserted"]} ) \
            .whenNotMatchedInsertAll() \
            .execute()
           
        print(f"Upsert completed for {table_name}")
    else:
        print(f"Table does not exists, creating new table {table_name}")
        df.write.format("delta").saveAsTable(table_name)



#define a set of endpoints with corresponding data_type and data_keys for Holman API without code_keys
holman_endpoints = {
    "vehicles": ("inventory", "clientVehicleNumber"),
    "accidents": ("accident", "clientVehicleNumber"),
    "odometer": ("odometerHistory", "vin"),
    "persons": ("person", "contactEmployeeId")
}

#iterate through the endpoints to fetch and upsert data
for data_type, (data_key, primary_key) in holman_endpoints.items():
    upd_endpoints = update_endpoints(data_type)
    print(f"Processing {data_type}")

    if upd_endpoints and upd_endpoints.get("run_all"):
        data_list = get_holman_data(token, data_type=data_type, data_key=data_key)
    elif upd_endpoints and "delta_url" in upd_endpoints:
        delta_url = upd_endpoints["delta_url"]
        # endpoint = f"{data_type}/{delta_url}"
        data_list = get_holman_data(token, data_type=data_type, data_key=data_key, url_ext=delta_url)
    else:
        print(f"Skipping {data_type} endpoint is not defined")
        continue
    
    data_list = replace_null_values(data_list)

    if data_list:
        Holman_Upsert_data(data_type, data_key, data_list, primary_key)
    else:
        print(f"No data found for {data_type}")
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

upd_endpoints = update_endpoints(data_type)

#
if upd_endpoints and upd_endpoints.get("run_all"):
    data_list = get_holman_data(token, data_type=data_type, data_key=data_key)

#creating dataframe from the data_list
df = spark.createDataFrame(data_list)
current_time = current_timestamp()
df = df.withColumn("tg_inserted", current_time).withColumn("tg_updated", current_time)

#doing an upsert while using merge key in merge condition.
# if table exists do an upsert and if table does not exist creating a new table
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
Getting Billing data from Holman API and processing the data
'''
data_type = "billing"
data_key = "billing"
primary_key = "vehicleNumber"
code_key = "billingTypeCode"
key_code = "invoiceDateCode"
data_list = []

# Retrieve endpoint options to get key values to get most recent data
endpoint_options = update_endpoints(data_type)
if endpoint_options is None:
    print(f"Skipping {data_type} endpoint is not defined")

#Retrieving key values from endpoint options for looping through the data
key_values =[endpoint_options["key_value"]] if "key_value" in endpoint_options else range(1,4)

d_path = f'/Volumes/{env}/bronze_vendor/holman/{data_type}'
dbutils.fs.mkdirs(d_path)

for code_value in range(1,4):
    for key_value in key_values:
    # key_value = 3
        print(f"\nChecking data for {data_type} with code key {code_key} = {code_value} and {key_code}={key_value}")

        # Retrieve total pages and skip if no pages are available
        total_pages = get_total_pages(data_type, code_key, code_value, key_code, key_value)
        if total_pages == 0:
            print(f"No pages available for {data_type} with code key {code_key} = {code_value}. Skipping...")
            continue
        print(f"Total Pages : {total_pages}")

        #Generating pagination urls for the data
        paginated_urls, num_batches = generate_paginated_urls(data_type,data_key, code_key, code_value,total_pages,key_value, key_code)
        print(f"Total URLs : {len(paginated_urls)}")

       
        #creating url datafarmes and batching the urls. 
        #Add part column to partition the pages into groups of 100
        #repartition the df based on num_batches and cache the df for faster processing
        #use udf to fetch the data from the url
        batch_df = spark.createDataFrame(paginated_urls)
        batch_df = batch_df.withColumn("part", floor(col("pageNumber") / 100)).repartition(num_batches, "part")
        result_df = batch_df.withColumn("data", fetch_data_udf(col("url"), col("pageNumber"), lit(data_type), lit(code_value), lit(d_path),lit(key_value))).cache()

        #extract data from result df and append to list
        data_list.extend(fetch_data_upsert(result_df, data_key))
        
        if data_list:
            #creating dataframe from data_list and doing an upsert
            Holman_Upsert_data(data_type,data_list, primary_key)
            print(f"Data upserted for data type {data_type} with code value {code_value}")
        

dbutils.fs.rm(d_path, True) #deleting the directory after processing the data


      
'''
Main execution loop for using udf_based parallel execution to fetch processing data
'''

# list of dictionaries where each dictionary contains the data type, data key, primary key for an endpoint
holman_coded_endpoints = [
    {"data_type": "maintenance", "code_key": "billPaidDateCode", "data_key": "maintenance", "primary_key": "clientVehicleNumber"},
    {"data_type": "violation", "code_key": "violationDateCode", "data_key": "violations", "primary_key": "record_id"},
     {"data_type": "fuels", "code_key": "transDateCode", "data_key": "us", "primary_key": "usRecordID"},
]
data_list = []


# Main execution loop for fetching and processing data
for endpoint_config in holman_coded_endpoints:
    data_type = endpoint_config["data_type"]
    code_key = endpoint_config["code_key"]
    data_key = endpoint_config["data_key"]
    primary_key = endpoint_config["primary_key"]

    # Retrieve endpoint options to get most recent data
    endpoint_options = update_endpoints(data_type)
    if endpoint_options is None:
        print(f"Skipping {data_type} endpoint is not defined")
        continue

    # code_values = [endpoint_options.get("code_value", i) for i in range(1,4) if "code_value" not in endpoint_options]
    code_values =[endpoint_options["code_value"]] if "code_value" in endpoint_options else range(1,4)

    # Setting up the directory path
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
        
        #Generating pagination urls,
        paginated_urls, num_batches = generate_paginated_urls(data_type,data_key, code_key, code_value,total_pages)

        #creating url datafarmes and batching the urls. 
        #Add part columnto partition the pages into groups of 100
        #repartition the df based on num_batches and cache the df for faster processing
        #use udf to fetch the data from the url

        print(f"Total URLs : {len(paginated_urls)}")
        batch_df = spark.createDataFrame(paginated_urls)
        batch_df = batch_df.withColumn("part", floor(col("pageNumber") / 100)).repartition(num_batches, "part")
        result_df = batch_df.withColumn("data", fetch_data_udf(col("url"), col("pageNumber"), lit(data_type), lit(code_value), lit(d_path))).cache()

        data_list.extend(fetch_data_upsert(result_df, data_key))
            
        if data_list:
            #creating dataframe from data_list and doing an upsert
            Holman_Upsert_data(data_type,data_list, primary_key) 
            print(f"Data upserted for data type {data_type} with code value {code_value}")

dbutils.fs.rm(d_path, True) #deleting the directory after processing the data



 


  
 
        
   
