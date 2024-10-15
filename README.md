[CANNOT_DETERMINE_TYPE] Some of types cannot be determined after inferring.


def fetch_events_meta_data():
    end_date = datetime.now().isoformat(timespec='milliseconds') + 'Z'
    page =1
    # from_date = "2024-07-01T16:19:58.0000Z"
    # to_date = "2024-09-30T16:19:58.0000Z"
    limit = 1000
    all_events_metadata=[]
    complex_columns = ['behaviors','coachingSessionNotes','eventNotes','notes', 'reviewNotes'] 
    table_name = f"bronze.lytx_video_eventsWithMetadata"
    

    if spark.catalog.tableExists(table_name):
        df_table = spark.table(table_name)
        last_update = df_table.agg(F.max("update_time")).collect()[0][0]
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
    df = df.withColumn("insert_time", current_time).withColumn("update_time", current_time)
    upsert_data(df,table_name,current_time,['behaviors','coachingSessionNotes','eventNotes','notes', 'reviewNotes'],'id',endpoint)
