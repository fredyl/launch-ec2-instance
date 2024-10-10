def upsert_data(df,table_name,current_time,complex_columns=None,primary_key=None,endpoint=None):
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
            .whenMatchedUpdate(condition=" OR ".join([f"existing_data.{col} != new_data.{col}" for col in df.columns if col not in complex_columns and col != primary_key and col != "tg_inserted"]),
                set={"tg_updated": current_time, 
                     **{col: F.col(f"new_data.{col}") for col in df.columns if col != primary_key and col != "tg_inserted"}}) \
            .whenNotMatchedInsert(values={**{col: F.col(f"new_data.{col}") for col in df.columns}}) \
            .execute()
        print(f"Upsert (merge) completed successfully for {table_name}.")
    else:
        print(f"Table {table_name} does not exist. Creating new table...")
        df.write.format("delta").mode("overwrite").saveAsTable(table_name)
        print(f"Table {table_name} created successfully with new data.")
