


def get_holman_api_response(token, endpoint):
    base_url = "https://customer-experience-api.arifleet.com/v1/"
    url = base_url + endpoint
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f"Bearer {token}"
    }
    # headers['Authorization'] = f"Bearer {token}"
    response = requests.get(url, headers=headers)
    if response.status_code == 204:
        print(f"Received 204, No Content from the API.")
        return response, None
    elif response.status_code == 200:
        print(f"Received 200, Success from the API.")
        response_data = response.json()
        return response, response_data
    elif response.status_code == 401:
        print(f"Authentication failed: {response.status_code}, {response.text}")
        token = get_token()
        time.sleep(5)
        return get_holman_api_response(token, endpoint)
    else:
        raise Exception("Failed:", response.status_code, response.text)

        
def get_all_pages(token,data_type, code_key, code_value):
    endpoint = f"{data_type}?{code_key}={code_value}"
    _, response_data  = get_holman_api_response(token, endpoint)
    if response_data:
        total_pages = int(response_data.get("totalPages", 1))
        print(f"Total pages: {total_pages}")
        return total_pages
    else:
        print("No pages found")


# Generates paginated URLs based on the total number of pages
def generate_paginated_urls(token, data_key, code_key, code_value, batch_size=200):
    base_url = "https://customer-experience-api.arifleet.com/v1/"
    total_pages = get_all_pages(token, data_type, code_key, code_value)
    if total_pages == 0:
        raise Exception("No pages found")

    pagination_urls = []
    loops = (total_pages // batch_size) + 1
    print(f" loops : {loops}")

    for l in range(loops):
        page = l + 1
        url = f"{base_url}{data_type}?{code_key}={code_value}"
        pagination_urls.append({"pageNumber": page, "url": url})
        # print(pagination_urls)

    return pagination_urls, loops



# Data upsert function for Delta table
def Holman_Upsert_data(data_type, data_key, data_list, primary_key):
    table_name = f"bronze.holman_{data_type}_{data_key}"
    df = spark.createDataFrame(data_list)
    current_time = current_timestamp()
    df = df.withColumn("tg_inserted", current_time).withColumn("tg_updated", current_time)
    # df.display()
    # Check for duplicates based on primary key
    dup_check_df = df.groupBy(primary_key).count().filter(col("count") > 1)
    dup_check_df.show()
    duplicate_id = [row[primary_key] for row in dup_check_df.collect()]
    duplicate_rows_df = df.filter(col(primary_key).isin(duplicate_id))
    duplicate_rows_df.show(100)
    if spark.catalog.tableExists(table_name):
        print(f"Table {table_name} exists. Performing upsert (merge)...")
        delta_table = DeltaTable.forName(spark, table_name)
        delta_table.alias("existing_data") \
            .merge(df.alias("new_data"), expr(f"new_data.{primary_key} = existing_data.{primary_key}")) \
            .whenMatchedUpdate(
                set={col_name: col(f"new_data.{col_name}") for col_name in df.columns if col_name != primary_key and col_name != "tg_inserted"}
            ) \
            .whenNotMatchedInsertAll() \
            .execute()
    else:
        print(f"Table does not exist, creating new table {table_name}")
        df.write.format("delta").saveAsTable(table_name)



def fetch_data_from_url(url):
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f"Bearer {token}"
    }
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.text  # Return the raw response text
        else:
            return f"Error: {response.status_code}"
    except Exception as e:
        return f"Exception: {str(e)}"

fetch_data_udf = udf(fetch_data_from_url, StringType())


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

        total_pages = get_all_pages(token, data_type, code_key, code_value)
        if total_pages == 0:
            print(f"No data found for {data_type} with code key {code_key} = {code_value}. Skipping...")

        batch_urls, loops =generate_paginated_urls(token, data_key, code_key, code_value, batch_size=200)
        batch_df = spark.createDataFrame(batch_urls)
        batch_df = batch_df.withColumn("part", floor(col("pageNumber") / 10)).repartition(loops, "part")


        output_df = batch_df.withColumn("data", fetch_data_udf(col("url"))).cache()

        # total_pages = get_all_pages(token="your_token", endpoint=f"{data_key}?{code_key}={code_value}")
        # pagination_urls = generate_paginated_urls("your_token", data_key, code_key, code_value, batch_size=200)
        
        data_list = []
        for row in output_df.collect():
            if row.data is not None and row.data != "Bad Response":
                try:
                    data_list.extend(json.loads(row.data)[data_key])
                except json.JSONDecodeError as e:
                    print(f"Error parsing JSON for URL: {row.urpageNumberl}: {e}")
            else:
                print(f"skipping page {row.pageNumber} due to None or Bad Response")

        if data_list:
            cleaned_data = replace_null_values(data_list)

            Holman_Upsert_data(data_type, data_key, cleaned_data, primary_key)
            print(f"Data upserted for data type {data_type} with code value {code_value}")
        else:
            print(f"No data found for data type {data_type} with code value {code_value}")
    
holman_coded_endpoints = [
    {"data_type": "billing", "code_key": "billingTypeCode", "data_key": "billing", "primary_key": "vehicleNumber"},
    # {"data_type": "fuels", "code_key": "transDateCode", "data_key": "can", "primary_key": "clientVehicleNumber"},
    # {"data_type": "fuels", "code_key": "transDateCode", "data_key": "us", "primary_key": "clientVehicleNumber"},
    # {"data_type": "violation", "code_key": "violationDateCode", "data_key": "violations", "primary_key": "violationNo"}
]

# Main execution loop with UDF-based parallel data fetching
for endpoint_config in holman_coded_endpoints:
    data_type = endpoint_config["data_type"]
    code_key = endpoint_config["code_key"]
    data_key = endpoint_config["data_key"]
    primary_key = endpoint_config["primary_key"]

    for code_value in range(1, 4):  # Loop through code_key values 1 to 3
        print(f"\nChecking data for {data_type} with code key {code_key} = {code_value}")
        
        # Retrieve total pages and skip if no pages are available
        total_pages = get_all_pages(token, data_type, code_key, code_value)
        print(f"Total pages for {code_key}_{code_value}: {total_pages}")
        if total_pages == 0:
            print(f"No pages available for {data_type} with code key {code_key} = {code_value}. Skipping...")
            continue

        batch_urls, loops =generate_paginated_urls(token, data_key, code_key, code_value, batch_size=200)
        batch_df = spark.createDataFrame(batch_urls)
        batch_df = batch_df.withColumn("part", floor(col("pageNumber") / 100)).repartition(loops, "part")
        # batch_df.show()

        # Apply the UDF to fetch data for each URL
        result_df = batch_df.withColumn("data", fetch_data_udf(col("url")))
        # result_df.show()

        # Collect and process each page's data
        data_list = []
        for row in result_df.collect():
            if row.data and not row.data.startswith("Error") and not row.data.startswith("Exception"):
                try:
                    json_data = json.loads(row.data)
                    if isinstance(json_data, dict):
                        page_data = json_data.get(data_key, [])
                        data_list.extend(page_data)
                    elif isinstance(json_data, list):
                        data_list.extend(json_data)
                    else:
                        print("Expected a dictionary but got a list.")
                except json.JSONDecodeError:
                    print("Failed to parse JSON response.")
            else:
                print(f"Error fetching data for URL: {row.url}, Response: {row.data}")


        # Upsert data if available
        if data_list:
            cleaned_data = replace_null_values(data_list)
            # print(cleaned_data)
            Holman_Upsert_data(data_type, data_key, cleaned_data, primary_key)
            print(f"Data upserted for data type {data_type} with code value {code_value}")
        else:
            print(f"No data to upsert for data type {data_type} with code value {code_value}")
