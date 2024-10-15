#the below codes handes the video events metadata with pagination and upsert

end_date = datetime.now().isoformat(timespec='milliseconds') + 'Z'
page =1
limit = 1000
all_events_metadata=[]
complex_columns = ['behaviors','coachingSessionNotes','eventNotes','notes', 'reviewNotes'] 
table_name = "bronze.lytx_video_eventsWithMetadata"


#check if the delta table exists to determine the start date  for fetching data
#aggregate to find the maximum value of tg_updated column, which will represent the most update time
#collect()[0][0] extracts the single value for the max tg_updated column from aggregate results
#convert last_update to iso format will millisecond precision needed by the API
#Append Z to indictate UTC timezone
if spark.catalog.tableExists(table_name):
    df_table = spark.table(table_name)#Reference the delta table
    last_update = df_table.agg(max("tg_updated")).collect()[0][0]
    print(f"last update time: {last_update}")
    start_date = last_update.isoformat(timespec='milliseconds') + 'Z'
    print(f"start date: {start_date}")
    print(f"end date: {end_date}")
else:
    #else if the delta table or data does not exist, set the start date to 90 days prior to the current date
    start_date = (datetime.now() - timedelta(days=90)).isoformat(timespec='milliseconds') + 'Z'

#pagination
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
    page +=1 #increment the page number by 1
print("Pagination completed")

#convert the data to dataframe, add timestamps columns and perform an upsert
df = spark.createDataFrame(all_events_metadata) 
current_time = current_timestamp()
df = df.withColumn("tg_inserted", current_time).withColumn("tg_updated", current_time)
upsert_data(df,table_name,current_time,['behaviors','coachingSessionNotes','eventNotes','notes', 'reviewNotes'],'id',endpoint)
