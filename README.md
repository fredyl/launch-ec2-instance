dbutils.fs.mkdirs('/Volumes/{env}/bronze_vendor/holmanP/{data_type}')


def fetch_data_from_url(url, page, data_type, code_value):
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f"Bearer {token}"
    }
    response = requests.get(url, headers=headers)
    filePath = f'{chk_path}{data_type}_{code_value}_{page}.json'
    if response.status_code != 200:
        return f"Error: {response.status_code}"
    
    # with open(chk_path, 'w') as f:
    #     f.write(response.text)

    with open(filePath, 'w') as f:
        f.write(response.text)
    
    return filePath


https://github.com/user-attachments/assets/6bbc53c6-224c-4822-8c4d-d79f197e4343




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


  from pyspark.sql.functions import col, sum as spark_sum

def Holman_Upsert_data(data_type, data_key, data_list, primary_key= None):
    
    table_name = f"bronze.holman_{data_type}_{data_key}"
    print(f"table_name : {table_name}")
    df = spark.createDataFrame(data_list)
    current_time = current_timestamp()
    df = df.withColumn("tg_inserted", current_time).withColumn("tg_updated", current_time)
    # display(df)
    # Check for duplicates based on primary key

    if data_type == 'vehicles':
        combined_key = "combined_key"
        df = df.withColumn(combined_key, concat(col(primary_key), lit("_"), col("deliveryDate")))
        primary_key = combined_key
    elif data_type == 'maintainance':
        combined_key = "combined_key"
        df = df.withColumn(combined_key, concat(col(primary_key), lit("_"), col("invoiceDate")))
        primary_key = combined_key
    elif data_type == 'billing':
        combined_key = "combined_key"
        df = df.withColumn(combined_key, concat(col(primary_key), lit("_"), col("record_id")))
        primary_key = combined_key
    else:
        pass
    
    
    dup_check_df = df.groupBy(primary_key).count().filter(col("count") > 1)
    dup_check_df.show()
    # duplicate_id = [row[primary_key] for row in dup_check_df.collect()]
    # duplicate_rows_df = df.filter(col(primary_key).isin(duplicate_id)).orderBy(col(primary_key).desc())
    # display(duplicate_rows_df)
    
    # # # duplicate_rows_df = df.filter(col(primary_key).isin(duplicate_id)).orderBy(col(primary_key).desc())
    # # # display(duplicate_rows_df)
    # # total_duplicates = dup_check_df.agg(spark_sum(col("count").cast("int"))).collect()[0][0]
    # # print(f"Total dubs {total_duplicates}")
    # # # df = df.dropDuplicates([primary_key])

    

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


from pyspark.sql.types import MapType
from pyspark.sql import Row


holman_coded_endpoints = [
   
    # {"data_type": "fuels", "code_key": "transDateCode", "data_key": "can", "primary_key": "clientVehicleNumber"},
   
    {"data_type": "violation", "code_key": "violationDateCode", "data_key": "violations", "primary_key": "record_id"},
    {"data_type": "billing", "code_key": "billingTypeCode", "data_key": "billing", "primary_key": "vehicleNumber"},
    #  {"data_type": "fuels", "code_key": "transDateCode", "data_key": "us", "primary_key": "usRecordID"},
]

# Main execution loop for fetching and processing data
for endpoint_config in holman_coded_endpoints:
    data_type = endpoint_config["data_type"]
    code_key = endpoint_config["code_key"]
    data_key = endpoint_config["data_key"]
    primary_key = endpoint_config["primary_key"]


    for code_value in range(1, 4):  # Looping through code_key values 1 to 3
    # code_value = 1
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
        result_df = batch_df.withColumn("data", fetch_data_udf(col("url"), col("pageNumber"), lit(data_type), lit(code_value))).cache()

        
        # batch_df = spark.read.json(f"{chk_path}")
        file_path = [row.data for row in result_df.select("data").collect()]
        output_df = spark.read.json(file_path)
        json_data = output_df.select(data_key).toJSON().collect()
        cleaned_data  = [replace_null_values_df(json.loads(row)) for row in json_data]
        output_df = spark.createDataFrame([Row(**item) for item in cleaned_data])

        




        data_list = []
        for row in output_df.collect():
            if data_key in row and isinstance(row[data_key], list):
                data_list.extend(row[data_key])

        # if data_list:
        Holman_Upsert_data(data_type, data_key,data_list, primary_key)
        print(f"Data upserted for data type {data_type} with code value {code_value}")

        for f in dbutils.fs.ls(chk_path):
            dbutils.fs.rm(f.path, True)
            



