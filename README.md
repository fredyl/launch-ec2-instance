def get_holman_api_response(token, endpoint, param_pagination = None):
    base_url = "https://customer-experience-api.arifleet.com/v1/"
    url = base_url + endpoint
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f"Bearer {token}"
    }
    # headers['Authorization'] = f"Bearer {token}"
    response = requests.get(url, headers=headers, params=param_pagination)
    if response.status_code == 204:
        print(f"Received 204, No Content from the API.")
        return response, None
    elif response.status_code == 200:
        response_data = response.json()
        return response, response_data
    elif response.status_code == 401:
        token = get_token()
        time.sleep(5)
        return get_holman_api_response(token, endpoint)
    else:
        raise Exception("Failed:", response.status_code, response.text)


def get_all_pages(token, data_type,endpoint):
    response, response_data  = get_holman_api_response(token, endpoint, param_pagination = None)
    if response.status_code == 200:
        response_data = response.json()
        total_pages = int(response_data.get("totalPages", 1))
        return total_pages
    else:
        raise Exception("Failed:", response.status_code, response.text)

def generate_paginated_urls(data_type, data_key, batch_size = 200):
    total_pages = get_all_pages(token, data_type, data_key)
    if total_pages == 0:
        raise Exception("No data found.")

    pagination_urls = []
    loops = (total_pages // batch_size) + 1
    for loop in range(loops):
        loop_value = loop * batch_size
        url = f"https://customer-experience-api.arifleet.com/v1/{data_key}?{code_key}={code}"
        pagination_urls.append(url)
    return pagination_urls



def Holman_Upsert_data(data_type, data_key, data_list, primary_key=None):
    
    table_name = f"bronze.holman_{data_type}_{data_key}"
    print(table_name)
    df = spark.createDataFrame(data_list)
    current_time = current_timestamp()
    df = df.withColumn("tg_inserted", current_time).withColumn("tg_updated", current_time)
    # df.display()
    dup_check_df = df.groupBy(primary_key).count().filter(col("count") > 1)
    display(dup_check_df)
    if spark.catalog.tableExists(table_name):
        print(f"Table {table_name} exists. Performing upsert (merge)...")
        delta_table = DeltaTable.forName(spark, table_name)
        delta_table.alias("existing_data") \
            .merge(
                df.alias("new_data"), expr(f"new_data.{primary_key} = existing_data.{primary_key}")
            ) \
            .whenMatchedUpdate(
                condition=" OR ".join([
                    f"existing_data.{col_name} != new_data.{col_name}" for col_name in df.columns 
                    if  col_name != primary_key and col_name != "tg_inserted"
                ]),  # Set tg_updated to current timestamp and update other columns from new data
                set={ "tg_updated": current_time, 
                    **{col_name: col(f"new_data.{col_name}") for col_name in df.columns if col_name != primary_key and col != "tg_inserted"}}
            ) \
            .whenNotMatchedInsert(
                values={**{col_name: col(f"new_data.{col_name}") for col_name in df.columns}}
            ) \
            .execute()
    else:
        print(f"Table does not exists, creating new table {table_name}")
        df.write.format("delta").saveAsTable(table_name)



def replace_null_values(item_list):
    for item in item_list:
        for key, value in item.items():
            if value == "null" or value is None:
                item[key] = ""
    return item_list



  #define a set of endpoints with corresponding data keys
holman_coded_endpoints =[
    {"data_type" : "billing", "code_key": "billingTypeCode","data_key": "billing","primary_key": "invoiceNumber" },
    {"data_type": "fuels","code_key": "transDateCode","data_key": "can","primary_key": "clientVehicleNumber"},
    {"data_type": "fuels","code_key": "transDateCode","data_key": "us","primary_key": "usRecordID"},
    {"data_type" : "violation","code_key": "violationDateCode","data_key": "violations","primary_key": "record_id"}
    ]


for endpoint_config in holman_coded_endpoints:
    data_type = endpoint_config["data_type"]
    code_key = endpoint_config["code_key"]
    data_key = endpoint_config["data_key"]
    primary_key = endpoint_config["primary_key"]


    for code_value in range(1,4):
        data_list = fetch_holman_code_batch(data_type, code_key, data_key,code_value)

        if data_list:
            cleaned_data = replace_null_values(data_list)
            Holman_Upsert_data(cleaned_data, data_type, primary_key)
            print(f"Data upserted for data type {data_type} with code value {code_value}")
        else:
            print(f"No data found for data type {data_type} with code value {code_value}")



      
        
