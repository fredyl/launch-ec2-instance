how do I include the below code in the upsert_data functions

if col not in complex_columns and col != primary_key and col != "tg_inserted"






def upsert_data(df, table_name, current_time, complex_columns=[], primary_key="id", endpoint=None):
    '''
    Generic Function that performs an upsert
    '''
    
    # Setting the value of [] to an empty list if not provided
    # complex_columns = complex_columns or []

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
                df.alias("new_data"), expr(f"new_data.{primary_key} = existing_data.{primary_key}")
            ) \
            .whenMatchedUpdate(
                set={ 
                    **{col_name: col(f"new_data.{col_name}") for col_name in df.columns if col_name != primary_key and col_name != "tg_inserted"}
                    }
            ) \
            .whenNotMatchedInsert(
                values={**{col_name: col(f"new_data.{col_name}") for col_name in df.columns}}
            ) \
            .execute()
        print(f"Upsert (merge) completed successfully for {table_name}.")
    
    #create a new table if table does not exists
    else:
        print(f"Table {table_name} does not exist. Creating new table...")
        df.write.format("delta").mode("overwrite").saveAsTable(table_name)
        print(f"Table {table_name} created successfully with new data.")
