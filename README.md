******************************************************************************************************************
*******************************************************************************************************************
# importing libaries from tglib
from tglib import ErrorHandler

eh = ErrorHandler(sendEmail = False)
env = spark.sql("SELECT current_catalog()").collect()[0][0]


chk_path = f'/Volumes/{env}/bronze_vendor/holmanP/{data_type}'
dbutils.fs.mkdirs(chk_path)


%sql
CREATE VOLUME IF NOT EXISTS dev.bronze_vendor.holmanP


def get_holman_api_response(token, endpoint):
    # global token
    base_url = "https://customer-experience-api.arifleet.com/v1/"
    url = base_url + endpoint
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f"Bearer {token}"
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 204:
        print(f"Received 204, No Content from the API.")
        return response, None
    elif response.status_code == 200:
        print(f"Received 200, Success from the API.")
        return response.json()
    elif response.status_code == 401:
        print(f"Authentication failed: {response.status_code}, {response.text}")
        token = get_token()
        time.sleep(5)
        return get_holman_api_response(token, endpoint)
    else:
        raise Exception("Failed:", response.status_code, response.text)


def get_total_pages(data_type, code_key, code_value):
    endpoint = f"{data_type}?{code_key}={code_value}"
    response_data  = get_holman_api_response(token, endpoint)
    if response_data:
        return int(response_data.get("totalPages", 1))
    return 0



# Generates paginated URLs based on the total number of pages
def generate_paginated_urls(data_type,data_key, code_key, code_value,total_pages,batch_size=200):
    base_url = "https://customer-experience-api.arifleet.com/v1/"
    pagination_urls = []
    num_batches = (total_pages // batch_size) + 1
    print(f" num_batches : {num_batches}")
    for i in range(num_batches):
        for page in range(i * batch_size + 1, min((i + 1) * batch_size + 1, total_pages +1)):
            url = f"{base_url}{data_type}?{code_key}={code_value}&pageNumber={page}"
            pagination_urls.append({"pageNumber": page, "url": url})
            # print(pagination_urls)
    print('len', len(pagination_urls))
    return pagination_urls, num_batches



def get_holman_data(token, data_type, data_key, ):
    #Define the endpoint and get the response and oing pagination
    
    limit=200
    page=1
    data_list = []

    #Pagination logic
    while True:
        endpoint = f"{data_type}?pageNumber={page}"
        response_data = get_holman_api_response(token, endpoint)
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

    


def fetch_data_from_url(url, page, data_type, code_value):
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f"Bearer {token}"
    }
    response = requests.get(url, headers=headers)
    filePath = f'{chk_path}/{data_type}_{code_value}_{page}.json'
    if response.status_code != 200:
        return f"Error: {response.status_code}"
    
    # with open(chk_path, 'w') as f:
    #     f.write(response.text)

    with open(filePath, 'w') as f:
        f.write(response.text)
    
    return filePath



fetch_data_udf = udf(fetch_data_from_url, StringType())



def replace_nulls_in_dataframe(df: DataFrame): 
    return df.select([when(col(c).isNull() | (col(c) == "null"), lit(" ")).otherwise(col(c)).alias(c) for c in df.columns])


def Holman_Upsert_data(data_type, data_key, data_list, primary_key= None):
    '''
    Getting dictionary to map data_type used to create a unique key to be used as primary key for data that seems similar
    and also do and upsert
    '''
    table_name = f"bronze.holman_{data_type}_{data_key}"
    print(f"table_name : {table_name}")
    df = spark.createDataFrame(data_list)
    current_time = current_timestamp()
    df = df.withColumn("tg_inserted", current_time).withColumn("tg_updated", current_time)

    #Dictionary to map data_type defined in key_col and columns concatenated
    key_col = {
        'vehicles': 'deliveryDate',
        'maintenance': 'record_id',
        'accidents': 'record_id',
        'billing': 'record_id'
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




def replace_null_values_df(data_key):
    if isinstance (data_key, dict):
        return {k: replace_null_values_df(v) if v is not None else " " for k, v in data_key.items()}
    elif isinstance (data_key, list):
        return [replace_null_values_df(item) if item is not None else " " for item in data_key]
    else:
        return data_key if data_key is not None else " "




#define a set of endpoint for corresponding data keys, creating a merge key used to match rows between the source df and delta table
data_type="orders" 
data_key="orderHistory"
primary_key="clientVehicleNumber"
merge_keys= ["clientVehicleNumber", "upfitPoIssueDate","tg_updated"]  
table_name = f"bronze.holman_{data_type}_{data_key}"
print(table_name)

#retrieve  data from get_holman_data
data_list = get_holman_data(token, data_type =data_type, data_key=data_key)
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
                **{col_name: col(f"new_data.{col_name}") for col_name in df.columns if col_name != primary_key and col_name != "tg_inserted"},
                "tg_updated": current_time
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
    # {"data_type": "violation", "code_key": "violationDateCode", "data_key": "violations", "primary_key": "record_id"},
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
    d_path = f'/Volumes/{env}/bronze_vendor/holmanP/{data_type}'
    dbutils.fs.mkdirs(d_path)


    for code_value in range(1, 4):  # Looping through code_key values 1 to 3

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

        #Upsert the data to the table
        Holman_Upsert_data(data_type, data_key,data_list, primary_key)
        print(f"Data upserted for data type {data_type} with code value {code_value}")

dbutils.fs.rm(d_path, True) #deleting the directory after processing the data
        


