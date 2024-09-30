def lytx_get_repoonse_from_event_api(endpoint):
    if "videos" in endpoint:
        base_url = "https://lytx-api.prod5.ph.lytx.com"
    else:
        base_url = "https://lytx-api.prod5.ph.lytx.com"
    url = base_url + endpoint
    headers = {'x-apikey': api_key }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        response_data = response.json()
        return response_data
    else:
        raise Exception("Failed:", response.status_code, response.text)


def process_api_data_to_delta(endpoint, table_name):
    response_data = lytx_get_repoonse_from_event_api(endpoint)

    if isinstance(response_data, list):
        df = spark.createDataFrame(response_data) 
    else:
        rdd = spark.sparkContext.parallelize([response_data])
        df = spark.read.json(rdd)
    current_time = F.current_timestamp()
    df = df.withColumn("insert_time", current_time).withColumn("update_time", current_time)

    if "id" in df.columns:
        primary_key = "id"
    else:
        raise Exception(f"No id column found in data schema for enppoint: {endpoint}")

    if spark.catalog.tableExists(table_name):
        print(f"Table {table_name} exists. Performing upsert (merge)...")
        delta_table = DeltaTable.forName(spark, table_name) 
        delta_table.alias("existing_data") \
          .merge(df.alias("new_data"), F.expr(f"new_data.{primary_key} = existing_data.{primary_key}")) \
          .whenMatchedUpdate(set={
              "update_time": current_time, 
              **{col: F.col(f"new_data.{col}") for col in df.columns if col != primary_key}
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


endpoints =[
    "/video/safety/events/statuses" ,
    "/video/safety/events/behaviors",
    "/video/safety/events/triggersubtypes",
    "/video/safety/events/triggers",
    "/video/safety/eventsWithMetadata",
    "/vehicles/statuses",
    "/vehicles/types",
]

for endpoint in endpoints:
    try:
        table_name = f"bronze.lytx_{endpoint.split('/')[1]}_{endpoint.split('/')[-1]}"
        print(f"Creating table {table_name}...")
        process_api_data_to_delta(endpoint, table_name)
    except Exception as e:
        print(f"Error with {endpoint}: {e}")


endpoint = "/vehicles/all"   
response_data = lytx_get_repoonse_from_event_api(endpoint)
if 'vehicles' not in response_data:
    raise Exception(f"No vehicles key found in response data for endpoint: {endpoint}")
vehicle_ids= [vehicle['id'] for vehicle in response_data['vehicles']]

response_data =[]
for vehicle_id in vehicle_ids:
    endpoint = f"/vehicles/{vehicle_id}"
    vehicle_data = lytx_get_repoonse_from_event_api(endpoint)
    response_data.append(vehicle_data)    
df = spark.createDataFrame(response_data)
current_time = F.current_timestamp()
df = df.withColumn("insert_time", current_time).withColumn("update_time", current_time)

if "id" in df.columns:
    primary_key = "id"
else:
    raise Exception(f"No id column found in data schema for enppoint: {endpoint}")
table_name = f"bronze.lytx_vehicles_vehicleId"

if spark.catalog.tableExists(table_name):
    print(f"Table {table_name} exists. Performing upsert (merge)...")
    delta_table = DeltaTable.forName(spark, table_name)  # Reference the Delta table in the database
    delta_table.alias("existing_data") \
        .merge(df.alias("new_data"), F.expr(f"new_data.{primary_key} = existing_data.{primary_key}")) \
        .whenMatchedUpdate(set={
            "update_time": current_time, 
            **{col: F.col(f"new_data.{col}") for col in df.columns if col != primary_key}
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


limit = 100
page = 1
all_vechicles=[]
while True:
    endpoint = f"/vehicles/all?limit={limit}&page={page}&includeSubgroups=true"
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
table_name = f"bronze.lytx_vehicles_vehicle_id"
if spark.catalog.tableExists(table_name):
    print(f"Table {table_name} exists. Performing upsert (merge)...")
    delta_table = DeltaTable.forName(spark, table_name)  # Reference the Delta table in the database
    delta_table.alias("existing_data") \
        .merge(df.alias("new_data"), F.expr(f"new_data.{primary_key} = existing_data.{primary_key}")) \
        .whenMatchedUpdate(set={
            "update_time": current_time, 
            **{col: F.col(f"new_data.{col}") for col in df.columns if col != primary_key}
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


end_date = datetime.now().date()
start_date = end_date - timedelta(days=2*365)
# print(start_date, end_date)
limit = 100
page = 1
all_events=[]
while True:
    endpoint = f"/video/events?StartDate={start_date}&EndDate={end_date}&PageNumber={page}&PageSize={limit}"
    response_data = lytx_get_repoonse_from_event_api(endpoint)
    print(f"getting data for page:{page}")
    if 'events' in response_data:
        events = response_data['events']
        all_events.extend(events)
        if len(events) < limit:
            break
    else:
        raise Exception(f"No vehicles key found in response data for endpoint: {endpoint}") 
    page += 1
df = spark.createDataFrame(all_events)  
current_time = F.current_timestamp()
spark_df = df.withColumn("insert_time", current_time).withColumn("update_time", current_time)
if "id" in df.columns:
    primary_key = "id"
else:
    raise Exception(f"No id column found in data schema for enppoint: {endpoint}")
table_name = f"bronze.lytx_video_events"
if spark.catalog.tableExists(table_name):
    print(f"Table {table_name} exists. Performing upsert (merge)...")
    delta_table = DeltaTable.forName(spark, table_name)  # Reference the Delta table in the database
    delta_table.alias("existing_data") \
        .merge(spark_df.alias("new_data"), F.expr(f"new_data.{primary_key} = existing_data.{primary_key}")) \
        .whenMatchedUpdate(set={
            "update_time": current_time, 
            **{col: F.col(f"new_data.{col}") for col in spark_df.columns if col != primary_key}
        }) \
        .whenNotMatchedInsert(values={
            **{col: F.col(f"new_data.{col}") for col in spark_df.columns}
        }) \
        .execute()
    print(f"Upsert (merge) completed successfully for {table_name}.")
else:
    print(f"Table {table_name} does not exist. Creating new table...")
    df.write.format("delta").mode("overwrite").saveAsTable(table_name)
    print(f"Table {table_name} created successfully with new data.")
