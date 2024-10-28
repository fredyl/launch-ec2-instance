import requests, json
from delta.tables import DeltaTable
from pyspark.sql.functions import col, expr, current_timestamp, desc,row_number,concat, lit
from pyspark.sql import Window
from delta.tables import DeltaTable
chk_path = '/Volumes/{env}/bronze_vendor/holman'


%sql
CREATE VOLUME IF NOT EXISTS dev.bronze_vendor.holman


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
        response_data = response.json()
        return response, response_data
    elif response.status_code == 401:
        token = get_token()
        time.sleep(5)
        return get_holman_api_response(token, endpoint)
    else:
        raise Exception("Failed:", response.status_code, response.text)


def replace_null_values(item_list):
    for item in item_list:
        for key, value in item.items():
            if value == "null" or value is None:
                item[key] = ""
    return item_list


global_checkpoint = {}
def save_checkpoint(endpoint_key, page_number):
    global global_checkpoint
    global_checkpoint[endpoint_key] = page_number
    print(f"Checkpoint Updated to {page_number}")


def get_last_checkpoint(endpoint_key, default=0):
    global global_checkpoint
    return global_checkpoint.get(endpoint_key, default)





def Holman_Upsert_data(data_type, data_key, data_list, primary_key=None):
    
    table_name = f"bronze.holman_{data_type}_{data_key}"
    print(table_name)
    df = spark.createDataFrame(data_list)
    current_time = current_timestamp()
    df = df.withColumn("tg_inserted", current_time).withColumn("tg_updated", current_time)
    # df.display()
    dup_check_df = df.groupBy(primary_key).count().filter(col("count") > 1)
    display(dup_check_df)
    # duplicate_id = [row[primary_key] for row in dup_check_df.collect()]
    # duplicate_rows_df = df.filter(col(primary_key).isin(duplicate_id)).orderBy(desc(primary_key))
    # duplicate_rows_df.show()
    # duplicate_rows = df.join(dup_check_df.select(primary_key), primary_key, how='inner')
    # dup_check_df.show(truncate=False)
    # df_partitioned = df.repartition('id')
    # df_sorted = df_partitioned.sortWithinPartitions('id',desc('lastConnected'))
    # df = df_sorted.dropDuplicates(subset=['id'])
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




def fetch_holman_code_batch_data(data_type, code_key, data_key, code, token, batch_size=200):
    data_list = []
    endpoint_key = f"Holman_{data_type}_{code_key}_{code}"
    last_page = get_last_checkpoint(endpoint_key, default=1)
    page = last_page
    pages_fetched = 0
    total_pages = None

    while True:
        if pages_fetched >= batch_size:
            # Save checkpoint
            save_checkpoint(endpoint_key, page)
            print(f"Fetched batch size {batch_size} from {endpoint_key}, saving checkpoint")
            return data_list  # Return the data list after fetching a batch
        
        print(f"Fetching page {page} of data from {endpoint_key}")
        endpoint = f"{data_type}?{code_key}={code}&pageNumber={page}"
        response, response_data = get_holman_api_response(token, endpoint)

        if response.status_code == 200:
            batch_data = response_data.get(data_key)

            if batch_data is None or len(batch_data) == 0:
                print(f"No data found for {data_type}_{data_key} on page {page}, stopping pagination")
                save_checkpoint(endpoint_key, page)
                break
                

            print(f"Processing page {page}, with {len(batch_data)} records")
            data_list.extend(batch_data)

            if total_pages is None:
                total_pages = int(response_data.get("totalPages", 1))
                print(f"Total pages: {total_pages}")
            if page >= total_pages:
                print("Stopping Pagination")

            page += 1
            pages_fetched += 1
        else:
            print(f"Error: {response.status_code} {response.text}")
            break

    # Save checkpoint if loop completes
    save_checkpoint(endpoint_key, page)
    return data_list


#define a set of endpoints with corresponding data keys
holman_coded_endpoints =[
    {
        "data_type" : "billing",
        "code_key": "billingTypeCode",
        "data_key": "billing",
        "primary_key": "invoiceNumber"
     },
    # {
    #     "data_type": "fuels",
    #     "code_key": "transDateCode",
    #     "data_key": "can",
    #     "primary_key": "clientVehicleNumber"
    # },
    # {
    #     "data_type": "fuels",
    #     "code_key": "transDateCode",
    #     "data_key": "us",
    #     "primary_key": "usRecordID"
    # },
    {
        "data_type" : "violation",
        "code_key": "violationDateCode",
        "data_key": "violations",
        "primary_key": "record_id"
    }
]
   

#iterate through the endpoints to fetch and upsert data
for endpoint_config in holman_coded_endpoints:
    data_type = endpoint_config["data_type"]
    code_key = endpoint_config["code_key"]
    data_key = endpoint_config["data_key"]
    primary_key = endpoint_config["primary_key"]
    code =2
    page =1
    for code in range(1,4):
    # while True:
        data_list = fetch_holman_code_batch_data(data_type, code_key, data_key,code,token, batch_size=200)
        if data_list is not None:
            clean_data_list = replace_null_values(data_list)
        # print(json.dumps(clean_data_list, indent=4))
            if clean_data_list:
                Holman_Upsert_data(data_type, data_key, clean_data_list, primary_key)
        else:
            print(f"No data found for {data_type}_{data_key}")
            break

