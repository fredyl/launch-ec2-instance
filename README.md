NameError: name 'current_time' is not defined
File <command-1663170670380898>, line 44
     41     df = df.withColumn("insert_time", current_time).withColumn("update_time", current_time)
     42     upsert_data(df,table_name,complex_columns='complex_columns',primary_key='id',endpoint=None)
---> 44 fetch_events_meta_data()
File <command-1663170670380898>, line 42, in fetch_events_meta_data()
     40 current_time = F.current_timestamp()
     41 df = df.withColumn("insert_time", current_time).withColumn("update_time", current_time)
---> 42 upsert_data(df,table_name,complex_columns='complex_columns',primary_key='id',endpoint=None)
File <command-1777769096121330>, line 18, in upsert_data(df, table_name, complex_columns, primary_key, endpoint)
     10     print(f"Table {table_name} exists. Performing upsert (merge)...")
     11     delta_table = DeltaTable.forName(spark, table_name)  # Reference the Delta table in the database
     12     delta_table.alias("existing_data") \
     13         .merge(df.alias("new_data"), F.expr(f"new_data.{primary_key} = existing_data.{primary_key}")) \
     14         .whenMatchedUpdate(condition=" OR ".join(
     15             [f"existing_data.{col} != new_data.{col}" for col in df.columns if col not in complex_columns and col != primary_key and col != "insert_time"]
     16         ),
     17             set={
---> 18                 "update_time": current_time, 
     19                 **{col: F.col(f"new_data.{col}") for col in df.columns if col != primary_key and col != "insert_time"}
     20             }) \
     21         .whenNotMatchedInsert(values={
     22             **{col: F.col(f"new_data.{col}") for col in df.columns}
     23         }) \
     24         .execute()
     25     print(f"Upsert (merge) completed successfully for {table_name}.")
     26 else:



         df = df.withColumn("insert_time", current_time).withColumn("update_time", current_time)
    upsert_data(df,table_name,complex_columns='complex_columns',primary_key='id',endpoint=None)


    def upsert_data(df,table_name,complex_columns=None,primary_key='id',endpoint=None):
    complex_columns = complex_columns or []

    if "id" in df.columns:
        primary_key = "id"
    else:
        raise Exception(f"No id column found in data schema for enppoint: {endpoint}")

    if spark.catalog.tableExists(table_name):
        print(f"Table {table_name} exists. Performing upsert (merge)...")
        delta_table = DeltaTable.forName(spark, table_name)  # Reference the Delta table in the database
        delta_table.alias("existing_data") \
            .merge(df.alias("new_data"), F.expr(f"new_data.{primary_key} = existing_data.{primary_key}")) \
            .whenMatchedUpdate(condition=" OR ".join(
                [f"existing_data.{col} != new_data.{col}" for col in df.columns if col not in complex_columns and col != primary_key and col != "insert_time"]
            ),
                set={
                    "update_time": current_time, 
                    **{col: F.col(f"new_data.{col}") for col in df.columns if col != primary_key and col != "insert_time"}
                }) \
            .whenNotMatchedInsert(values={
                **{col: F.col(f"new_data.{col}") for col in df.columns}
            }) \
            .execute()
        print(f"Upsert (merge) completed successfully for {table_name}.")
    else:
        print(f"Table {table_name} does not exist. Creating new table...")
        df.write.format("delta").mode("overwrite").saveAsTable(table_name)
        print(f"Table {table_name} created successfully with new data.")
