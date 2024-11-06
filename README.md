def replace_null_values_df(data_key):
    if isinstance (data_key, dict):
        return {k: replace_null_values_df(v) if v is not None else " " for k, v in data_key.items()}
    elif isinstance (data_key, list):
        return [replace_null_values_df(item) if item is not None else " " for item in data_key]
    else:
        return data_key if data_key is not None else " "



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


holman_endpoints = {
    "vehicles": ("inventory", "clientVehicleNumber"),
    "maintenance": ("maintenance", "clientVehicleNumber"),
    "accidents": ("accident", "record_id"),
    "odometer": ("odometerHistory", "vin"),
    "persons": ("person", "contactEmployeeId")
}

#iterate through the endpoints to fetch and upsert data
for data_type, (data_key, primary_key) in holman_endpoints.items():
    data_list = get_holman_data(token, data_type=data_type, data_key=data_key)
    data_list = replace_null_values(data_list)
    # print(json.dumps(data_list, indent=4))
    if data_list:
        Holman_Upsert_data(data_type, data_key, data_list, primary_key)
    else:
        print(f"No data found for {data_type}_{data_key}")
print("All data fetched")


data_type="orders" 
data_key="orderHistory"
primary_key="clientVehicleNumber"
merge_keys= ["clientVehicleNumber", "upfitPoIssueDate","tg_updated"]  
table_name = f"bronze.holman_{data_type}_{data_key}"
print(table_name)

data_list = get_holman_data(token, data_type =data_type, data_key=data_key)
df = spark.createDataFrame(data_list)
current_time = current_timestamp()
df = df.withColumn("tg_inserted", current_time).withColumn("tg_updated", current_time)
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
    df.write.format("delta").saveAsTable(table_name)




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
