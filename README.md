[DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE] Cannot perform Merge as multiple source rows matched and attempted to modify the same
target row in the Delta table in possibly conflicting ways. By SQL semantics of Merge,
when multiple source rows match on the same target row, the result may be ambiguous
as it is unclear which source row should be used to update or delete the matching
target row. You can preprocess the source table to eliminate the possibility of
multiple matches. Please refer to


limit = 100
page = 1
all_vehicle_data=[]
complex_columns = ['devices']

while True:
    endpoint = f"/video/vehicles?PageNumber={page}&PageSize={limit}"
    response_data = lytx_get_repoonse_from_event_api(endpoint)
    # print(json.dumps(response_data, indent=2))
    print(f"getting data for page:{page}")
    if "vehicles" not in response_data:
        raise Exception(f"No vehicles key found in response data for endpoint: {endpoint}")

    vehicles = response_data['vehicles']
    all_vehicle_data.extend(vehicles)
    # print(json.dumps(all_vehicle_data, indent=2))

    if len(vehicles) < limit:
        break
    page += 1

if all_vehicle_data:
    for vehicles in all_vehicle_data:
        vehicles['devices'] = json.dumps(vehicles['devices'])
    df = spark.createDataFrame(all_vehicle_data)
    current_time = F.current_timestamp()
    df = df.withColumn("insert_time", current_time).withColumn("update_time", current_time)
    df.display()
    if "id" in df.columns:
        primary_key = "id"
    else:
        raise Exception(f"No id column found in data schema for enppoint: {endpoint}")
    table_name = f"bronze.lytx_video_vehicles_vehicleId"
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
