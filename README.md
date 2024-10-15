

def upsert_data(df, table_name, current_time, complex_columns=None, primary_key="id", endpoint=None):
    '''
    Generic Function that performs an upsert
    '''
    
    # Setting the value of [] to an empty list if not provided
    complex_columns = complex_columns or []

    # Check if primary_key exist in dataframe column. If not raises an exception
    if primary_key not in df.columns:
        raise Exception(f"No id column found in data schema for endpoint: {endpoint}")

    if spark.catalog.tableExists(table_name):  # Check if table exists
        print(f"Table {table_name} exists. Performing upsert (merge)...")
        delta_table = DeltaTable.forName(spark, table_name)  # Reference the Delta table in the database
        # Performing merge operation between existing data and new data
        #when data matched update existing data if there are any changes in non_complex columns and columns that are not #primary keys
        delta_table.alias("existing_data") \
            .merge(
                df.alias("new_data"), F.expr(f"new_data.{primary_key} = existing_data.{primary_key}")
            ) \
            .whenMatchedUpdate(
                condition=" OR ".join([
                    f"existing_data.{col} != new_data.{col}" for col in df.columns 
                    if col not in complex_columns and col != primary_key and col != "tg_inserted"
                ]),  # Set tg_updated to current timestamp and update other columns from new data
                set={ "tg_updated": current_time, 
                    **{col: F.col(f"new_data.{col}") for col in df.columns if col != primary_key and col != "tg_inserted"}}
            ) \
            .whenNotMatchedInsert(
                values={**{col: F.col(f"new_data.{col}") for col in df.columns}}
            ) \
            .execute()
        print(f"Upsert (merge) completed successfully for {table_name}.")
    
    #create a new table if table does not exists
    else:
        print(f"Table {table_name} does not exist. Creating new table...")
        df.write.format("delta").mode("overwrite").saveAsTable(table_name)
        print(f"Table {table_name} created successfully with new data.")Error: [CANNOT_DETERMINE_TYPE] Some of types cannot be determined after inferring.
