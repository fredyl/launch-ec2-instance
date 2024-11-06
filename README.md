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
