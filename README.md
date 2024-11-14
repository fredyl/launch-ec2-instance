

import requests,json,time
from datetime import datetime, timedelta
from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp, col, expr,desc, max
api_key = dbutils.secrets.get(scope="BIHubScope",key="Lytx-API-Key")


def lytx_get_repoonse_from_event_api(endpoint):
    #the function retrieves data from Latyx API if there using the provided endpoint

    for attempt in range(3):
        
        base_url = "https://lytx-api.prod5.ph.lytx.com"
        url = base_url + endpoint #combine base url with endpoint
        headers = {'x-apikey': api_key } #defining header for the API request
        response = requests.get(url, headers=headers)#sending a GET request to the API
        
        #if the response is 204, it means there is no data to be retrieved
        if response.status_code == 204:
            print(f" Recieved 204, No Content from the API.")
            return  None
        elif response.status_code == 200:
            return response
        elif 500 <= response.status_code < 600 :#if response is between 500 and 600 while excluding, raise an exception
            retry_after = int(response.headers.get('Retry-After', 8))
            time.sleep(retry_after) #wait for the specified time and retry repeat if the response is 500 twice
            if attempt == 2: 
                raise Exception(f"Failed after 3 attempts with status code: {response.status_code}, response: {response.text}")
        else:
            response.raise_for_status()


            

def upsert_data(df, table_name, current_time, complex_columns=[], primary_key="id", endpoint=None):
    '''
    Generic Function that performs an upsert
    '''
    
    # Setting the value of [] to an empty list if not provided
    # complex_columns = complex_columns or []

    # Check if primary_key exist in dataframe column. If not raises an exception
    if primary_key not in df.columns:
        raise Exception(f"No id column found in data schema for endpoint: {endpoint}")

    if spark.catalog.tableExists(table_name):  # Check if table exists
        print(f"Table {table_name} exists. Performing upsert (merge)...")
        delta_table = DeltaTable.forName(spark, table_name)  # Reference the Delta table in the database

        #separate the columns into non_complex and complex columns for update
        non_complex_columns = [col_name for col_name in df.columns
                if col_name not in complex_columns and col_name != primary_key and col_name != "tg_inserted"]
        
        # Performing merge operation between existing data and new data
        #defining columns to be updated, when data matched update existing data
        delta_table.alias("existing_data") \
            .merge(
                df.alias("new_data"), expr(f"new_data.{primary_key} = existing_data.{primary_key}")
            ) \
            .whenMatchedUpdate(
                #update only non_complex_columns by comparing values
                #always update complex_column without comparing value since complex columns are nested data
                set={ 
                    **{col_name: col(f"new_data.{col_name}") for col_name in non_complex_columns},
                    **{col_name: col(f"new_data.{col_name}") for col_name in complex_columns},
                }
            ) \
            .whenNotMatchedInsertAll() \
            .execute()
        print(f"Upsert (merge) completed successfully for {table_name}.")
    
    #create a new table if table does not exists
    else:
        print(f"Table {table_name} does not exist. Creating new table...")
        df.write.format("delta").mode("overwrite").saveAsTable(table_name)
        print(f"Table {table_name} created successfully with new data.")

        

def process_api_data_to_delta(endpoint, table_name):
    '''
    This generic function is used to process the data from the api and insert or update the data to the delta table. 
    '''
    response = lytx_get_repoonse_from_event_api(endpoint) # fetch data from lytx api
    response_data = response.json()
    print(len(response_data))
    if isinstance(response_data, list): #check if response data is a list or not and creates a dataframe
        df = spark.createDataFrame(response_data) 
    else:
        #if data is not a list Parallelize the data to a rdd
        rdd = spark.sparkContext.parallelize([response_data]) 
        df = spark.read.json(rdd)
        
    #get current time stamp and add the columns tg_updated and tg_inserted to the dataframe and perform an upsert to delta table
    current_time = current_timestamp()
    df = df.withColumn("tg_inserted", current_time).withColumn("tg_updated", current_time)
    upsert_data(df,table_name,current_time)


    



'''
The below codes handles the various latyx api endpoints call
'''

#List of API endpoints
endpoints =[
    "/video/safety/events/statuses" ,
    "/video/safety/events/behaviors",
    "/video/safety/events/triggersubtypes",
    "/video/safety/events/triggers",
    "/vehicles/statuses",
    "/vehicles/types",
]
#loop through the API endpoints and process the data to delta table
for endpoint in endpoints:
    try:
        table_name = f"bronze.lytx_{endpoint.split('/')[1]}_{endpoint.split('/')[-1]}"
        print(f"Creating table name: {table_name}...")

        #Call the function to process the data and save it into the delta table
        process_api_data_to_delta(endpoint, table_name)
    except Exception as e:
        print(f"Error with {endpoint}: {e}")


        


def get_lytx_paging_data(endpoint, page, limit, start_date=None, end_date=None):
    '''
    This function handles the pagination for the lytx API for various endpoints.
    '''
    all_data = []
    while True:
        # Initiating endpoints for pagination
        if "/vehicles/all" in endpoint:
            page_endpoint = f"/vehicles/all?limit={limit}&page={page}&includeSubgroups=true"
            process_key = "vehicles"
        elif "/video/vehicles" in endpoint:
            page_endpoint = f"/video/vehicles?PageNumber={page}&PageSize={limit}"
            process_key = "vehicles"
        elif "/video/events" in endpoint:
            page_endpoint = f"/video/events?StartDate={start_date}&EndDate={end_date}&PageNumber={page}&PageSize={limit}"
            process_key = "events"
        elif "/video/safety/eventsWithMetadata" in endpoint:
            page_endpoint = f"/video/safety/eventsWithMetadata?from={start_date}&to={end_date}&dateOption=lastUpdatedDate&sortDirection=desc&sortBy=lastUpdatedDate&includeSubgroups=true&limit={limit}&page={page}"
            process_key = None
        else:
            raise Exception(f"Invalid endpoint: {endpoint}")

        response = lytx_get_repoonse_from_event_api(page_endpoint)

        if response is None or response.status_code == 204:  # response is empty or status code is 204 stop paging
            print(f"No Content on page {page}, Stopping Pagination")
            break

        response_data = response.json()

        if response.status_code == 200:
            if process_key is not None:
                key_data = response_data[process_key]
            else:
                key_data = response_data
            print(f"Processing page {page}, with {len(key_data)} records")
            all_data.extend(key_data)
            if not key_data:
                break
        else:
            break
        page += 1  # move to next page
    print("Pagination complete")
    return all_data

'''
The below codes handes the vehicles data with pagination and upsert
'''
limit = 100
page = 1
all_vehicles=[]

#defining the table name and endpoint
table_name = "bronze.lytx_vehicles_vehicle_id"
endpoint = f"/vehicles/all?limit={limit}&page={page}&includeSubgroups=true"
all_vehicles = get_lytx_paging_data(endpoint, page, limit)
df = spark.createDataFrame(all_vehicles)

#create dataframe and get current time stamp and add the columns tg_updated and tg_inserted to the dataframe
#drop duplicates if exists and do upsert
current_time = current_timestamp()
df = df.withColumn("tg_inserted", current_time).withColumn("tg_updated", current_time)
df_partitioned = df.repartition('id')
df_sorted = df_partitioned.sortWithinPartitions('id',desc('lastConnected'))
df = df_sorted.dropDuplicates(subset=['id'])
upsert_data(df,table_name,current_time)




'''
The below codes handes the video vehicles data with pagination and upsert
'''
limit = 100
all_vehicles=[]
page =1
table_name = "bronze.lytx_video_vehicles_vehicleId"
endpoint = f"/video/vehicles?PageNumber={page}&PageSize={limit}"
all_vehicles = get_lytx_paging_data(endpoint, page, limit)

#create dataframe and get current time stamp and add the columns tg_updated and tg_inserted to the dataframe
#drop duplicates if exists and do upsert
data_dict = json.dumps(all_vehicles)
rdd = spark.sparkContext.parallelize([data_dict])
spark_df = spark.read.json(rdd) 
current_time = current_timestamp()
df = spark_df.withColumn("tg_inserted", current_time).withColumn("tg_updated", current_time)
df_partitioned = df.repartition('id')
df_sorted = df_partitioned.sortWithinPartitions('id',desc('lastCommunication'))
df = df_sorted.dropDuplicates(subset=['id'])
upsert_data(df,table_name,current_time)


def replace_null_values(item_list):
    return [{k: (v if v not in["null", None] else "") for k, v in item.items()} for item in item_list]

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

endpoint = f"/video/safety/eventsWithMetadata?from={start_date}&to={end_date}&dateOption=lastUpdatedDate&sortDirection=desc&sortBy=lastUpdatedDate&includeSubgroups=true&limit={limit}&page={page}"



#pagination
all_events_metadata = get_lytx_paging_data(endpoint, page, limit, start_date, end_date)
clean_data = replace_null_values(all_events_metadata)

# convert the data to dataframe, add timestamps columns and perform an upsert
if clean_data:
    df = spark.createDataFrame(clean_data)
    current_time = current_timestamp()
    df = df.withColumn("tg_inserted", current_time).withColumn("tg_updated", current_time)
    upsert_data(df,table_name,current_time,complex_columns,'id',endpoint)
else:
    print("No data recieved from API")



#Handles video events API with complex columns pagination and upsert

end_date = datetime.now().date()
table_name = "bronze.lytx_video_events"
limit = 100
page = 1
all_events=[]

#check if the delta table exists to determine the start date  for fetching data
#aggregate find the maximum value of tg_updated column, which represents most updated time
#collect()[0][0] extracts the single value for the max tg_updated column from aggregate results
if spark.catalog.tableExists(table_name): 
    df_table = spark.table(table_name)
    last_update = df_table.agg(max("tg_updated")).collect()[0][0]
    print(f"last update time: {last_update}")
    start_date = last_update.date().isoformat()     #let the start date be the last update time
    print(f"start date: {start_date}")
    print(f"end date: {end_date}")
else:
    start_date = (end_date - timedelta(days=90)).isoformat()

endpoint = f"/video/events?StartDate={start_date}&EndDate={end_date}&PageNumber={page}&PageSize={limit}"

#pagination
all_events = get_lytx_paging_data(endpoint, page, limit, start_date, end_date)
#convert data to dataframe and upsert to the table
if all_events:
    df = spark.createDataFrame(all_events)
    current_time = current_timestamp()
    df = df.withColumn("tg_inserted", current_time).withColumn("tg_updated", current_time)
    upsert_data(df,table_name,current_time,['deviceViews'] )
    
else:
    print("No new data found")
