end_date = datetime.now().date().isoformat()
table_name = f"bronze.lytx_video_events"
limit = 100
page = 1
all_events=[]
complex_columns = ['deviceViews'] 
# complex_columns = [field.name for field in df.schema.fields if isinstance(field.dataType,   
#      (ArrayType, MapType))]
# end_date = "2024-09-30"
# start_date = "2023-09-30"

    
if spark.catalog.tableExists(table_name):
    df_table = spark.table(table_name)
    last_update = df_table.agg(F.max("update_time")).collect()[0][0]
    print(f"last update time: {last_update}")
    start_date = last_update
    print(f"start date: {start_date}")
    print(f"end date: {end_date}")
else:
    start_date = end_date - timedelta(days=90)


while True:
    endpoint = f"/video/events?StartDate={start_date}&EndDate={end_date}&PageNumber={page}&PageSize={limit}"
    response_data = lytx_get_repoonse_from_event_api(endpoint)
    print(f"getting data for page:{page}")
    # print(json.dumps(response_data, indent=2))
    if 'events' in response_data:
        events = response_data['events']
        all_events.extend(events)
        if len(events) < limit:
            break
    else:
        raise Exception(f"No vehicles key found in response data for endpoint: {endpoint}") 
    page += 1

if all_events:
    df = spark.createDataFrame(all_events)
    current_time = F.current_timestamp()
    df = df.withColumn("insert_time", current_time).withColumn("update_time", current_time)
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
    
else:
    print("No new data found")
