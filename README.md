def get_lytx_paging_data(endpoint, page, limit):
    '''
    This function handles the pagination for the lytx api for /video/vehocles and /vehicles/all endpoints
    '''
    all_vehicles = []
    while True:
        #initiating endpoints for pagination
        if "/vehicles/all" in endpoint:
            page_endpoint = f"/vehicles/all?limit={limit}&page={page}&includeSubgroups=true"    
        elif "/video/vehicles" in endpoint:
            page_endpoint = f"/video/vehicles?PageNumber={page}&PageSize={limit}"
        else:
            raise Exception(f"Invalid endpoint: {endpoint}")
        response = lytx_get_repoonse_from_event_api(page_endpoint)
        response_data = response.json()
        status_code = response.status_code
        if response_data is None or status_code ==204: #response is empty or status code is 204 stop paging
            print(f" No Content on page {page}, Stopping Pagination")
            break
        if status_code == 200 and 'vehicles' in response_data:
            vehicles = response_data['vehicles']
            print(f"Processing page{page}, with {len(vehicles)} records")
            all_vehicles.extend(vehicles) 
        else:
            break
        page +=1 #move to next page
    print("Pagination completed")
    return all_vehicles


'''
The below codes handes the vehicles data with pagination and upsert
'''
limit = 100
page = 1
all_vechicles=[]

#defining the table name and endpoint
table_name = "bronze.lytx_vehicles_vehicle_id"
endpoint = f"/vehicles/all?limit={limit}&page={page}&includeSubgroups=true"
all_vehicles = get_lytx_paging_data(endpoint, page, limit)
df = spark.createDataFrame(all_vehicles)

#create dataframe and get current time stamp and add the columns tg_updated and tg_inserted to the dataframe
#drop duplicates if exists and do upsert
current_time = current_timestamp()
df = df.withColumn("tg_inserted", current_time).withColumn("tg_updated", current_time)
dup_check_df = df.groupBy("id").count().filter(col("count") > 1)
dup_check_df.show(truncate=False)
df_partitioned = df.repartition('id')
df_sorted = df_partitioned.sortWithinPartitions('id',desc('lastConnected'))
df = df_sorted.dropDuplicates(subset=['id'])
upsert_data(df,table_name,current_time,primary_key='id',endpoint=None)
