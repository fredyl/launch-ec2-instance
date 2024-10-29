from pyspark.sql.functions import col, current_timestamp
from delta.tables import DeltaTable
import requests

# Fetch API response function with pagination support
def get_holman_api_response(token, endpoint, param_pagination=None):
    base_url = "https://customer-experience-api.arifleet.com/v1/"
    url = base_url + endpoint
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f"Bearer {token}"
    }
    response = requests.get(url, headers=headers, params=param_pagination)
    if response.status_code == 204:
        print("Received 204, No Content from the API.")
        return response, None
    elif response.status_code == 200:
        return response, response.json()
    elif response.status_code == 401:
        token = get_token()
        time.sleep(5)
        return get_holman_api_response(token, endpoint)
    else:
        raise Exception("Failed:", response.status_code, response.text)

# Function to get total pages
def get_all_pages(token, endpoint):
    _, response_data = get_holman_api_response(token, endpoint)
    total_pages = int(response_data.get("totalPages", 1)) if response_data else 0
    return total_pages

# Generates paginated URLs based on the total number of pages
def generate_paginated_urls(data_key, code_key, code_value, total_pages):
    pagination_urls = []
    for page in range(1, total_pages + 1):
        url = f"https://customer-experience-api.arifleet.com/v1/{data_key}?{code_key}={code_value}&page={page}"
        pagination_urls.append(url)
    return pagination_urls

# Data upsert function for Delta table
def Holman_Upsert_data(data_type, data_key, data_list, primary_key):
    table_name = f"bronze.holman_{data_type}_{data_key}"
    df = spark.createDataFrame(data_list)
    current_time = current_timestamp()
    df = df.withColumn("tg_inserted", current_time).withColumn("tg_updated", current_time)
    
    if spark.catalog.tableExists(table_name):
        print(f"Table {table_name} exists. Performing upsert (merge)...")
        delta_table = DeltaTable.forName(spark, table_name)
        delta_table.alias("existing_data") \
            .merge(df.alias("new_data"), expr(f"new_data.{primary_key} = existing_data.{primary_key}")) \
            .whenMatchedUpdate(
                condition=" OR ".join([f"existing_data.{col_name} != new_data.{col_name}" for col_name in df.columns if col_name != primary_key and col_name != "tg_inserted"]),
                set={ "tg_updated": current_time, **{col_name: col(f"new_data.{col_name}") for col_name in df.columns if col_name != primary_key and col_name != "tg_inserted"}}
            ) \
            .whenNotMatchedInsert(
                values={col_name: col(f"new_data.{col_name}") for col_name in df.columns}
            ) \
            .execute()
    else:
        print(f"Table does not exist, creating new table {table_name}")
        df.write.format("delta").saveAsTable(table_name)

# Function to replace null values in data
def replace_null_values(item_list):
    for item in item_list:
        for key, value in item.items():
            if value == "null" or value is None:
                item[key] = ""
    return item_list

# Configuration for endpoints with corresponding data keys
holman_coded_endpoints = [
    {"data_type": "billing", "code_key": "billingTypeCode", "data_key": "billing", "primary_key": "invoiceNumber"},
    {"data_type": "fuels", "code_key": "transDateCode", "data_key": "can", "primary_key": "clientVehicleNumber"},
    {"data_type": "fuels", "code_key": "transDateCode", "data_key": "us", "primary_key": "usRecordID"},
    {"data_type": "violation", "code_key": "violationDateCode", "data_key": "violations", "primary_key": "record_id"}
]

# Loop to fetch and upsert data for each endpoint and code value
for endpoint_config in holman_coded_endpoints:
    data_type = endpoint_config["data_type"]
    code_key = endpoint_config["code_key"]
    data_key = endpoint_config["data_key"]
    primary_key = endpoint_config["primary_key"]
    
    for code_value in range(1, 4):  # Looping through code_key values 1 to 3
        total_pages = get_all_pages(token="your_token", endpoint=f"{data_key}?{code_key}={code_value}")
        pagination_urls = generate_paginated_urls(data_key, code_key, code_value, total_pages)
        
        data_list = []
        for url in pagination_urls:
            _, response_data = get_holman_api_response("your_token", url)
            if response_data and data_key in response_data:
                data_list.extend(response_data[data_key])

        if data_list:
            cleaned_data = replace_null_values(data_list)
            Holman_Upsert_data(data_type, data_key, cleaned_data, primary_key)
            print(f"Data upserted for data type {data_type} with code value {code_value}")
        else:
            print(f"No data found for data type {data_type} with code value {code_value}")
