 get_holman_orders_data(data_type="orders", data_key="orderHistory", primary_key="clientVehicleNumber"):  
    table_name = f"bronze.holman_{data_type}_{data_key}"
    print(table_name)
    data_list = fetch_Holman_code_data(data_key, "orderHistory", "clientVehicleNumber")
    df = spark.createDataFrame(data_list)
    current_time = current_timestamp()
    df = df.withColumn("tg_inserted", current_time).withColumn("tg_updated", current_time)
    if spark.catalog.tableExists(table_name):
        print(f"Table {table_name} exists. Performing upsert (merge)...")
        delta_table = DeltaTable.forName(spark, table_name)
        delta_table.alias("existing_data") \
            .merge(
                df.alias("new_data"), expr(f"new_data.{primary_key} = existing_data.{primary_key}")
            ) \
            .whenMatchedUpdate(
                set={
                    **{col_name: col(f"new_data.{col_name}") for col_name in df.columns if col_name != primary_key and col_name != "tg_inserted"},
                    "tg_updated": current_time
                }
            ) \
            .whenNotMatchedInsert(
                values={**{col_name: col(f"new_data.{col_name}") for col_name in df.columns}}
            ) \
            .execute()
        print(f"Upsert completed for {table_name}")
    else:
        print(f"Table does not exist, creating new table {table_name}")
        df.write.format("delta").saveAsTable(table_name)
