def get_token():
    url = "https://customer-experience-api.arifleet.com/v1/users/authenticate"
    payload = {
        "Username" : "9AD0ED33263920833DC6E47924C3A80BCECD4D86",
        "Password" : "Ap4PGA9f"
    }
    headers = {
        'Content-Type': 'application/json'
    }
    response = requests.post(url, headers=headers, data=json.dumps(payload))
    if response.status_code == 200:
        token = response.json().get("token")
        return token
    else:
        print(f"Authentication failed: {response.status_code}, {response.text}")

token = get_token()


def get_holman_api_response(token, endpoint):
    base_url = "https://customer-experience-api.arifleet.com/v1/"
    url = base_url + endpoint
    headers = {
        'Content-Type': 'application/json'
    }
    headers['Authorization'] = f"Bearer {token}"
    response = requests.get(url, headers=headers)
    if response.status_code == 204:
        print(f" Recieved 204, No Content from the API.")
        return response, None
    if response.status_code == 200:
        response_data = response.json()
        return response, response_data
    else:
        raise Exception("Failed:", response.status_code, response.text)



def get_holman_data(toke, data_type, data_key, ):
    limit=200
    page=1
    data_list = []

    while True:
        endpoint = f"{data_type}?pageNumber={page}"
        response, response_data = get_holman_api_response(token, endpoint)
        if response.status_code == 200 and response_data:
            batch_data = response_data.get(data_key, [])
            if len(batch_data) == 0:
                print(f"No more records on page {page}, Stopping Pagination")
                break
            data_list.extend(batch_data)
            print(f"Processing page{page}, with {len(batch_data)} records")
            page +=1
        else:
            break
    print(f"endpoint:{endpoint}")
    print(len(data_list))
    
    return data_list


def fetch_Holman_code_data(data_type, code_key, data_key,code):
    data_list = []
    # for code in range(1,3):
    # print(f"Fetching data from endpoint code={code}")
    page = 1
    while True:
        print(f"Fetching page {page} of data from endpoint code={code}")
        endpoint = f"{data_type}?{code_key}={code}&pageNumber={page}"
        response, response_data = get_holman_api_response(token, endpoint)
        if not response_data or data_key not in response_data or not response_data[data_key]:
            print(f"No more records on page {page}, Stopping Pagination")
            break
        if response.status_code == 200:
            fetched_data = response_data[data_key]
            data_list.extend(fetched_data)
        if int(response_data['pageNumber']) >= int(response_data['totalPages']):
            break
        page +=1
    return data_list




def replace_null_values(item_list):
    for item in item_list:
        for key, value in item.items():
            if value == "null" or value is None:
                item[key] = ""
    return item_list

def Holman_Upsert_data(data_type, data_key, data_list, primary_key=None):
    
    table_name = f"bronze.holman_{data_type}_{data_key}"
    print(table_name)
    df = spark.createDataFrame(data_list)
    current_time = current_timestamp()
    df = df.withColumn("tg_inserted", current_time).withColumn("tg_updated", current_time)
    
    # Check for duplicates based on primary key
    dup_check_df = df.groupBy(primary_key).count().filter(col("count") > 1)
    dup_check_df.display()
    duplicate_id = [row[primary_key] for row in dup_check_df.collect()]
    df = df.filter(~col(primary_key).isin(duplicate_id))
    if spark.catalog.tableExists(table_name):
        print(f"Table {table_name} exists. Performing upsert (merge)...")
        delta_table = DeltaTable.forName(spark, table_name)
        delta_table.alias("existing_data") \
            .merge(
                df.alias("new_data"), expr(f"new_data.{primary_key} = existing_data.{primary_key}")
            ) \
            .whenMatchedUpdate(
                  # Set tg_updated to current timestamp and update other columns from new data
                set={
                    **{col_name: col(f"new_data.{col_name}") for col_name in df.columns if col_name != primary_key and col != "tg_inserted"}}
            ) \
            .whenNotMatchedInsert(
                values={**{col_name: col(f"new_data.{col_name}") for col_name in df.columns}}
            ) \
            .execute()
        print(f"Upsert completed for {table_name}")
    else:
        print(f"Table does not exists, creating new table {table_name}")
        df.write.format("delta").saveAsTable(table_name)


#define a set of endpoints with corresponding data keys
holman_endpoints = {
    "accidents": ("accident","record_id"),
    "Odometer": ("odometerHistory","vin"),
    "orders": "order",
    "persons": ("person","contactEmployeeId")
}

#iterate through the endpoints to fetch and upsert data
for data_type,( data_key, primary_key) in holman_endpoints.items():
    data_list = get_holman_data(token, data_type =data_type, data_key=data_key)
    data_list = replace_null_values(data_list)
    # print(json.dumps(data_list, indent=4))
    if data_list:
        Holman_Upsert_data(data_type, data_key, data_list, primary_key)
    else:
        print(f"No data found for {data_type}_{data_key}")
else:
    print("All data fetched")



holman_endpoints = {
    # "accidents": "accident",
    # "Odometer": "odometerHistory",
    "orders": "order",
    # "persons": "person",
}

#iterate through the endpoints to fetch and upsert data
for data_type, data_key in holman_endpoints.items():
    data_list = get_holman_data(token, data_type =data_type, data_key=data_key)
    print(json.dumps(data_list, indent=4))
    # if data_list:
    #     Holman_Upsert_data(data_type, data_key, data_list, primary_key)
    # else:
    #     print(f"No data found")
else:
    print("All data fetched")


