
def Holman_Upsert_data(data_type, data_list, primary_key= None,merge_keys=None):
    '''
    Getting dictionary to map data_type used to create a unique key to be used as primary key for data that seems similar
    and then do an Upsert
    '''
    table_name = f"bronze.holman_{data_type}"
    df = spark.createDataFrame(data_list)
    current_time = current_timestamp()
    df = df.withColumn("tg_inserted", current_time).withColumn("tg_updated", current_time)

    #Dictionary to map data_type defined in key_col and columns concatenated
    key_col = {
        'vehicles': 'deliveryDate',
        'maintenance': 'record_id',
        'accidents': 'record_id',
        'odometer': 'record_id',
        'billing': 'record_id',
    }

    # Check if the data_type is in the dictionary key_col and concatenate columns if it is
    if data_type in key_col:
        combined_key = "combined_key"
        df = df.withColumn(combined_key, concat(col(primary_key), lit("_"), col(key_col[data_type])))
        primary_key = combined_key
    
    #drop duplicate records for persons data_type
    if data_type == "persons":
        df = df.dropDuplicates([primary_key])
    if not merge_keys:
        merge_keys = [primary_key] 

    #Upsert logic
    if spark.catalog.tableExists(table_name):
        print(f"Table {table_name} exists. Performing upsert (merge)...")
        delta_table = DeltaTable.forName(spark, table_name)
        delta_table.alias("existing_data")\
            .merge(df.alias("new_data"), expr(f"new_data.{primary_key} = existing_data.{primary_key}")) \
            .whenMatchedUpdate( set={col_name: col(f"new_data.{col_name}") for col_name in df.columns if col_name not in merge_keys and col_name != "tg_inserted"} ) \
            .whenNotMatchedInsertAll() \
            .execute()
           
        print(f"Upsert completed for {table_name}")
    else:
        print(f"Table does not exists, creating new table {table_name}")
        df.write.format("delta").saveAsTable(table_name)


'''
define a set of endpoint for corresponding data keys, creating a merge key used to match rows between the source df and delta table
'''

data_type="orders" 
data_key="orderHistory"
primary_key="clientVehicleNumber"
merge_keys= ["clientVehicleNumber", "upfitPoIssueDate","tg_updated"]  
table_name = f"bronze.holman_{data_type}"


upd_endpoints = update_endpoints(data_type)

#
if upd_endpoints and upd_endpoints.get("run_all"):
    data_list = get_holman_data(token, data_type=data_type, data_key=data_key)

Holman_Upsert_data(data_type,data_list, primary_key, merge_keys)
print(f"Data upserted for data type {data_type} with code value {code_value}")


[DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE] Cannot perform Merge as multiple source rows matched and attempted to modify the same
