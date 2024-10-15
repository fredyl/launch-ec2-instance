#Handles video events API with complex columns pagination and upsert

end_date = datetime.now().date().isoformat()
table_name = "bronze.lytx_video_events"
limit = 100
page = 1
all_events=[]
complex_columns = ['deviceViews'] 


#check if the delta table exists to determine the start date  for fetching data
#aggregate find the maximum value of tg_updated column, which represents most updated time
#collect()[0][0] extracts the single value for the max tg_updated column from aggregate results
if spark.catalog.tableExists(table_name): 
    df_table = spark.table(table_name)
    last_update = df_table.agg(max("tg_updated")).collect()[0][0]
    print(f"last update time: {last_update}")
    start_date = last_update     #let the start date be the last update time
    print(f"start date: {start_date}")
    print(f"end date: {end_date}")
else:
    start_date = end_date - timedelta(days=90)

#doing pagination
while True:
    endpoint = f"/video/events?StartDate={start_date}&EndDate={end_date}&PageNumber={page}&PageSize={limit}"
    response,response_data = lytx_get_repoonse_from_event_api(endpoint)
    print(f"getting data for page:{page}")

    if response_data is None:
        print(f" recieved 204, No Content on page {page}, Stopping Pagination")
        break
    if 'events' in response_data: #check if events key word is in response data
        events = response_data['events']
        all_events.extend(events)
        if not events:
                break
    else:
        raise Exception(f"No vehicles key found in response data for endpoint: {endpoint}") 
    page += 1 #increment the page number by 1
            

#convert data to dataframe and upsert to the table
if all_events:
    df = spark.createDataFrame(all_events)
    current_time = current_timestamp()
    df = df.withColumn("tg_inserted", current_time).withColumn("tg_updated", current_time)
    upsert_data(df,table_name,current_time,['deviceViews'],'id',endpoint)
    
else:
    print("No new data found")
