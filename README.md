I am having the below records when running the below codes and why 

Checking data for billing with code key billingTypeCode = 1
Authentication failed: 401, {"message":"Unauthorized"}
Received 200, Success from the API.
Total Pages : 1133
 loops : 6
Total URLs : 1133
batch_df : 1133
+----------+-----------------------------------------------------------------------------+----+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|pageNumber|url                                                                          |part|data                                                                                                                                                                                                                                                                                      |
+----------+-----------------------------------------------------------------------------+----+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|160       |https://customer-experience-api.arifleet.com/v1/billing?1=1133&pageNumber=160|16  |{"message":"0: records were submitted. 1: errors","entries":[{"originalRecord":"{\r\n  \"pageLimit\": 200,\r\n  \"pageNumber\": 160\r\n}","errorMessages":"Missing billingTypeCode, "}],"user_Ref_Token":null,"dbMessage":null,"validatedRecords":"[]","recordsCount":0,"stageSaved":null}|
|161       |https://customer-experience-api.arifleet.com/v1/billing?1=1133&pageNumber=161|16  |{"message":"0: records were submitted. 1: errors","entries":[{"originalRecord":"{\r\n  \"pageLimit\": 200,\r\n  \"pageNumber\": 161\r\n}","errorMessages":"Missing billingTypeCode, "}],"user_Ref_Token":null,"dbMessage":null,"validatedRecords":"[]","recordsCount":0,"stageSaved":null}|
|162       |https://customer-experience-api.arifleet.com/v1/billing?1=1133&pageNumber=162|16  |{"message":"0: records were submitted. 1: errors","entries":[{"originalRecord":"{\r\n  \"pageLimit\": 200,\r\n  \"pageNumber\": 162\r\n}","errorMessages":"Missing billingTypeCode, "}],"user_Ref_Token":null,"dbMessage":null,"validatedRecords":"[]","recordsCount":0,"stageSaved":null}|
|163       |https://customer-experience-api.arifleet.com/v1/billing?1=1133&pageNumber=163|16  |{"message":"0: records were submitted. 1: errors","entries":[{"originalRecord":"{\r\n  \"pageLimit\": 200,\r\n  \"pageNumber\": 163\r\n}","errorMessages":"Missing billingTypeCode, "}],"user_Ref_Token":null,"dbMessage":null,"validatedRecords":"[]","recordsCount":0,"stageSaved":null}|

def get_holman_api_response(token, endpoint):
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



def get_total_pages(token,data_type, code_key, code_value):
    endpoint = f"{data_type}?{code_key}={code_value}"
    response_data  = get_holman_api_response(token, endpoint)
    if response_data:
        return int(response_data.get("totalPages", 1))
        print
    return 0



    def generate_paginated_urls(token, data_key, code_key, code_value, batch_size=200):
    base_url = "https://customer-experience-api.arifleet.com/v1/"
    pagination_urls = []
    loops = (total_pages // batch_size) + 1
    print(f" loops : {loops}")
    for l in range(loops):
        for page in range(l * batch_size + 1, min((l + 1) * batch_size + 1, total_pages +1)):
            url = f"{base_url}{data_type}?{code_key}={code_value}&pageNumber={page}"
            pagination_urls.append({"pageNumber": page, "url": url})
    return pagination_urls, loops

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
                set={col_name: col(f"new_data.{col_name}") for col_name in df.columns if col_name not in [primary_key, "tg_inserted"]}
            ) \
            .whenNotMatchedInsertAll() \
            .execute()
    else:
        print(f"Table does not exist, creating new table {table_name}")
        df.write.format("delta").saveAsTable(table_name)


fetch_data_udf = udf(lambda url: fetch_data_from_url(url), StringType())


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


def replace_null_values(item_list):
    return [{k:(v if v is not["null", None] else " ") for k, v in item.items()} for item in item_list]



  holman_coded_endpoints = [
    {"data_type": "billing", "code_key": "billingTypeCode", "data_key": "billing", "primary_key": "invoiceNumber"},
    # {"data_type": "fuels", "code_key": "transDateCode", "data_key": "can", "primary_key": "clientVehicleNumber"},
    # {"data_type": "fuels", "code_key": "transDateCode", "data_key": "us", "primary_key": "usRecordID"},
    # {"data_type": "violation", "code_key": "violationDateCode", "data_key": "violations", "primary_key": "record_id"}
]

# Main execution loop for fetching and processing data
for endpoint_config in holman_coded_endpoints:
    data_type = endpoint_config["data_type"]
    code_key = endpoint_config["code_key"]
    data_key = endpoint_config["data_key"]
    primary_key = endpoint_config["primary_key"]

    for code_value in range(1, 4):  # Looping through code_key values 1 to 3
        print(f"\nChecking data for {data_type} with code key {code_key} = {code_value}")

        # Retrieve total pages and skip if no pages are available
        total_pages = get_total_pages("your_token", data_type, code_key, code_value)
        print(f"Total Pages : {total_pages}")
        if total_pages == 0:
            print(f"No pages available for {data_type} with code key {code_key} = {code_value}. Skipping...")
            continue
        
        paginated_urls, loops = generate_paginated_urls(data_key, code_key, code_value,total_pages)
        print(f"Total URLs : {len(paginated_urls)}")
        batch_df = spark.createDataFrame(paginated_urls)
        print(f"batch_df : {batch_df.count()}")

        #partition data and fect using udf
        batch_df = batch_df.withColumn("part", floor(col("pageNumber") / 10)).repartition(loops, "part")
        result_df = batch_df.withColumn("data", fetch_data_udf(col("url"))).cache()
        result_df.show(20, truncate=False)  
