def Holman_Upsert_data(data_type, data_key, data_list, primary_key):
    df = spark.createDataFrame(data_list)
    table_name = f"{data_type}_{data_key}"
    
    if spark.catalog.tableExists(table_name):
        print(f"Table {table_name} exists. Performing upsert (merge)...")
        delta_table = DeltaTable.forName(spark, table_name)
        delta_table.alias("existing_data") \
            .merge(
                df.alias("new_data"),
                f"new_data.{primary_key} = existing_data.{primary_key}"
            ) \
            .whenMatchedUpdate(
                set={
                    **{col_name: col(f"new_data.{col_name}") for col_name in df.columns if col_name != primary_key}
                }
            ) \
            .whenNotMatchedInsert(
                values={
                    **{col_name: col(f"new_data.{col_name}") for col_name in df.columns}
                }
            ) \
            .execute()
    else:
        print(f"Table {table_name} does not exist. Creating new table and inserting data...")
        df.write.format("delta").saveAsTable(table_name)
