[[CANNOT_INFER_TYPE_FOR_FIELD] Unable to infer the type of the field `devices`.](https://lytx-api.prod5.ph.lytx.com/video/safety/eventsWithMetadata?from=2022-10-31T15%3A04%3A30.000Z&to=2024-10-01T15%3A04%3A30.777Z&dateOption=lastUpdatedDate&sortDirection=desc&sortBy=lastUpdatedDate&includeSubgroups=true&limit=1000)


limit = 100
page = 1
all_vechicles=[]
complex_columns = ['deviceViews']
while True:
    endpoint = f"/video/vehicles?PageNumber={page}&PageSize={limit}"
    response_data = lytx_get_repoonse_from_event_api(endpoint)
    print(f"getting data for page:{page}")
    if 'vehicles' in response_data:
        vehicles = response_data['vehicles']
        all_vechicles.extend(vehicles)
        if len(vehicles) < limit:
            break
    else:
        raise Exception(f"No vehicles key found in response data for endpoint: {endpoint}") 
    page += 1

df = spark.createDataFrame(all_vechicles)
current_time = F.current_timestamp()
df = df.withColumn("insert_time", current_time).withColumn("update_time", current_time)
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
