def process_api_data_to_delta(endpoint, table_name):
    response,response_data = lytx_get_repoonse_from_event_api(endpoint)
    print(len(response_data))
    if isinstance(response_data, list):
        df = spark.createDataFrame(response_data) 
    else:
        rdd = spark.sparkContext.parallelize([response_data])
        df = spark.read.json(rdd)
    current_time = F.current_timestamp()
    df = df.withColumn("tg_inserted", current_time).withColumn("tg_updated", current_time)
    upsert_data(df,table_name,current_time, complex_columns=None,primary_key='id',endpoint=None)



endpoints =[
    "/video/safety/events/statuses" ,
    "/video/safety/events/behaviors",
    "/video/safety/events/triggersubtypes",
    "/video/safety/events/triggers",
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

def get_lytx_paging_data(endpoint, limit, page):
    all_vechicles = []
    while True:
        paged_endpoint = f"{endpoint}&page={page}"
        response,response_data = lytx_get_repoonse_from_event_api(paged_endpoint)
        status_code = response.status_code
        if not response_data:
            print(f" No Content on page {page}, Stopping Pagination")
            break
        if status_code == 200 and 'vehicles' in response_data:
            vehicles = response_data['vehicles']
            print(f"Processing page{page}, with {len(vehicles)} records")
            all_vechicles.extend(vehicles)
        else:
            break
        page +=1
    print("Pagination completed")
    return all_vechicles

limit = 100
page = 1
all_vechicles=[]
table_name = f"bronze.lytx_vehicles_vehicle_id"
endpoint = f"/vehicles/all?limit={limit}&includeSubgroups=true"

all_vechicles = get_lytx_paging_data(endpoint, limit, page)
df = spark.createDataFrame(all_vechicles)
df.display()
current_time = F.current_timestamp()
df = df.withColumn("tg_inserted", current_time).withColumn("tg_updated", current_time)
dup_check_df = df.groupBy("id").count().filter(F.col("count") > 1)
dup_check_df.show(truncate=False)
df_partitioned = df.repartition('id')
df_sorted = df_partitioned.sortWithinPartitions('id',F.desc('lastConnected'))
df = df_sorted.dropDuplicates(subset=['id'])
upsert_data(df,table_name,current_time,primary_key='id',endpoint=None)


limit = 100
page = 1
all_vechicles=[]
table_name = f"bronze.lytx_video_vehicles_vehicleId"
endpoint = f"/video/vehicles?PageSize={limit}"

all_vechicles = get_lytx_paging_data(endpoint, limit= PageSize, page=PageNumber)
df = spark.createDataFrame(all_vechicles)
df.display()
all_vechicles = get_lytx_paging_data(endpoint, limit, page)
while True:
    
    response,response_data = lytx_get_repoonse_from_event_api(endpoint)
    print(f"getting data for page:{page}")
    if 'vehicles' in response_data:
        vehicles = response_data['vehicles']
        all_vechicles.extend(vehicles)
        if len(vehicles) < limit:
            break
    else:
        raise Exception(f"No vehicles key found in response data for endpoint: {endpoint}") 
    page += 1

data_dict = json.dumps(all_vechicles)
rdd = spark.sparkContext.parallelize([data_dict])
spark_df = spark.read.json(rdd) 
current_time = F.current_timestamp()
df = spark_df.withColumn("tg_inserted", current_time).withColumn("tg_updated", current_time)
dup_check_df = df.groupBy("id").count().filter(F.col("count") > 1)
dup_check_df.show(truncate=False)
df_partitioned = df.repartition('id')
df_sorted = df_partitioned.sortWithinPartitions('id',F.desc('lastCommunication'))
df = df_sorted.dropDuplicates(subset=['id'])
upsert_data(df,table_name,current_time, primary_key='id',endpoint=None)



end_date = datetime.now().isoformat(timespec='milliseconds') + 'Z'
page =1
limit = 1000
all_events_metadata=[]
complex_columns = ['behaviors','coachingSessionNotes','eventNotes','notes', 'reviewNotes'] 
table_name = f"bronze.lytx_video_eventsWithMetadata"


if spark.catalog.tableExists(table_name):
    df_table = spark.table(table_name)
    last_update = df_table.agg(F.max("tg_updated")).collect()[0][0]
    print(f"last update time: {last_update}")
    start_date = last_update.isoformat(timespec='milliseconds') + 'Z'
    print(f"start date: {start_date}")
    print(f"end date: {end_date}")
else:
    start_date = (datetime.now() - timedelta(days=90)).isoformat(timespec='milliseconds') + 'Z'

while True:
    endpoint =f"/video/safety/eventsWithMetadata?from={start_date}&to={end_date}&dateOption=lastUpdatedDate&sortDirection=desc&sortBy=lastUpdatedDate&includeSubgroups=true&limit={limit}&page={page}"
    response,response_data = lytx_get_repoonse_from_event_api(endpoint)
    status_code = response.status_code
    if response_data is None:
        print(f" recieved 204, No Content on page {page}, Stopping Pagination")
        break
    if status_code == 200 and response_data:
        print(f"Processing page{page}, with {len(response_data)} records")
        all_events_metadata.extend(response_data)
    else:
        break
    page +=1
print("Pagination completed")

df = spark.createDataFrame(all_events_metadata) 
current_time = F.current_timestamp()
df = df.withColumn("tg_inserted", current_time).withColumn("tg_updated", current_time)
upsert_data(df,table_name,current_time,['behaviors','coachingSessionNotes','eventNotes','notes', 'reviewNotes'],'id',endpoint)



end_date = datetime.now().date().isoformat()
table_name = f"bronze.lytx_video_events"
limit = 100
page = 1
all_events=[]
complex_columns = ['deviceViews'] 

if spark.catalog.tableExists(table_name):
    df_table = spark.table(table_name)
    last_update = df_table.agg(F.max("tg_updated")).collect()[0][0]
    print(f"last update time: {last_update}")
    start_date = last_update
    print(f"start date: {start_date}")
    print(f"end date: {end_date}")
else:
    start_date = end_date - timedelta(days=90)


while True:
    endpoint = f"/video/events?StartDate={start_date}&EndDate={end_date}&PageNumber={page}&PageSize={limit}"
    response,response_data = lytx_get_repoonse_from_event_api(endpoint)
    print(f"getting data for page:{page}")
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
    df = df.withColumn("tg_inserted", current_time).withColumn("tg_updated", current_time)
    upsert_data(df,table_name,current_time,['deviceViews'],'id',endpoint)
    
else:
    print("No new data found")
