holman_endpoints = {
    "vehicles": ("inventory", "clientVehicleNumber"),
    # "accidents": ("accident", "clientVehicleNumber"),
    # "odometer": ("odometerHistory", "vin"),
    # "persons": ("person", "contactEmployeeId")
}

if "vehicles" in holman_endpoints:
    schema = StructType([
    StructField("orderReceivedDate",StringType(),True),
    StructField("orderPlacedDate",StringType(),True),
    StructField("scheduledProductionDate",StringType(),True),
    StructField("deliveryPaperMailed",StringType(),True),
    StructField("shipDate",StringType(),True),
    StructField("deliveredToDealerDate",StringType(),True),
    StructField("notifiedOfDelivery",StringType(),True),
    StructField("upfitInvoiceDate",StringType(),True)

])

#iterate through the endpoints to fetch and upsert data
for data_type, (data_key, primary_key) in holman_endpoints.items():
    upd_endpoints = update_endpoints(data_type)
    print(f"Processing {data_type}")

    if upd_endpoints and upd_endpoints.get("run_all"):
        data_list = get_holman_data(token, data_type=data_type, data_key=data_key)
    elif upd_endpoints and "delta_url" in upd_endpoints:
        delta_url = upd_endpoints["delta_url"]
        # endpoint = f"{data_type}/{delta_url}"
        data_list = get_holman_data(token, data_type=data_type, data_key=data_key, url_ext=delta_url)
    else:
        print(f"Skipping {data_type} endpoint is not defined")
        continue
    # print(json.dumps(data_list, indent=4))
    
    # data_list = replace_null_values(data_list)
    if data_list:
        Holman_Upsert_data(data_type, data_list, primary_key, merge_keys=None, schema=schema)
    else:
        print(f"No data found for {data_type}")
print("All data fetched")


def Holman_Upsert_data(data_type, data_list, primary_key= None,merge_keys=None,schema=None):
    '''
    Getting dictionary to map data_type used to create a unique key to be used as primary key for data that seems similar
    and then do an Upsert
    '''
    table_name = f"bronze.holman_{data_type}"

    if schema:
        df = spark.createDataFrame(data_list, schema=schema)
    else:
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
    if merge_keys:
        merge_condition =" AND " .join([f"new_data.{primary_key} = existing_data.{primary_key}" for primary_key in      merge_keys])
    else:
        merge_condition = f"new_data.{primary_key} = existing_data.{primary_key}"

    #Upsert logic
    if spark.catalog.tableExists(table_name):
        print(f"Table {table_name} exists. Performing upsert (merge)...")
        delta_table = DeltaTable.forName(spark, table_name)
        delta_table.alias("existing_data")\
            .merge(df.alias("new_data"), expr(merge_condition)) \
            .whenMatchedUpdate( set={col_name: col(f"new_data.{col_name}") for col_name in df.columns if col_name not in [primary_key, "tg_inserted"]} ) \
            .whenNotMatchedInsertAll() \
            .execute()
           
        print(f"Upsert completed for {table_name}")
    else:
        print(f"Table does not exists, creating new table {table_name}")
        df.write.format("delta").saveAsTable(table_name)
