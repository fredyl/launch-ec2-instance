import requests,json
import pyspark.sql.functions as F
from delta.tables import DeltaTable
api_key = "t6nu2xUgxpYyA3OUbzpxO6atDQYj1Lxy"

def lytx_get_repoonse_from_event_api(endpoint):
    base_url = "https://lytx-api.prod5.ph.lytx.com/video"
    url = base_url + endpoint
    headers = {
        'x-apikey': api_key
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        response_data = response.json()
        return response_data
        # print(json.dumps(response, indent =2))
    else:
        raise Exception("Failed:", response.status_code, response.text)


def statuses_api_response():
    endpoint = "/safety/events/statuses"   
    response_data = lytx_get_repoonse_from_event_api(endpoint)
    return response_data
    print(response_data)
statuses_api_response()


def triggers_api_response():
    endpoint = "/safety/events/triggers"
    response_data = lytx_get_repoonse_from_event_api(endpoint)
    return response_data
    print(response_data)
triggers_api_response()


def triggersubtypes_api_response():
    endpoint = "/safety/events/triggersubtypes"
    response_data = lytx_get_repoonse_from_event_api(endpoint)
    return response_data
    print(response_data)

triggersubtypes_api_response()

def behaviors_api_response():
    endpoint = "/safety/events/behaviors"
    response_data = lytx_get_repoonse_from_event_api(endpoint)
    return response_data
    print(response_data)

behaviors_api_response()



def lytx_get_repoonse_from_event_api(endpoint):
    base_url = "https://lytx-api.prod5.ph.lytx.com/video"
    url = base_url + endpoint
    headers = {
        'x-apikey': api_key
    }
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
        table_name = f"bronze.lytx_{endpoint.split('/')[1]}_{endpoint.split('/')[-1]}_table"
        print(f"Creating table {table_name}...")
        process_api_data_to_delta(endpoint, table_name)
    except Exception as e:
        print(f"Error with {endpoint}: {e}")

# def generic_lytx_apiCall_response(endpoint):
#     response_data = lytx_get_repoonse_from_event_api(endpoint)
#     print(f"\n Response for endpoint: {endpoint}")
#     print("=" * (len(f"Response for endpoint: {endpoint}")))
#     print(json.dumps(response_data, indent =2))
#     return response_data


def fetch_vehicle_details():
    # Step 1: Fetch all vehicles and extract IDs
    all_vehicles_endpoint = "/vehicles/all"
    response_data = lytx_get_repoonse_from_event_api(all_vehicles_endpoint)
    
    # Extract vehicle IDs from the response
    if 'vehicles' not in response_data:
        raise Exception("No 'vehicles' key found in the response data")
    
    vehicle_ids = [vehicle['id'] for vehicle in response_data['vehicles']]  # List of vehicle IDs
    
    # Step 2: Fetch details for each vehicle by ID
    vehicle_details = []
    for vehicle_id in vehicle_ids:
        vehicle_endpoint = f"/vehicles/{vehicle_id}"
        vehicle_data = lytx_get_repoonse_from_event_api(vehicle_endpoint)  # Fetch details by ID
        vehicle_details.append(vehicle_data)  # Store the vehicle details
    
    return vehicle_details  # Return the list of all vehicle details



vehicles_vehicleId_api_response
********************************

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
if isinstance(response_data, list):
    df = spark.createDataFrame(response_data)
current_time = F.current_timestamp()
df = df.withColumn("insert_time", current_time).withColumn("update_time", current_time)
if "id" in df.columns:
    primary_key = "id"
else:
    raise Exception(f"No id column found in data schema for enppoint: {endpoint}")
table_name = f"bronze.lytx_vehicles_vehicleId_table"
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



video_vehicleId_api_response
****************************

endpoint = "/video/vehicles"   
response_data = lytx_get_repoonse_from_event_api(endpoint)

if 'vehicles' not in response_data:
    raise Exception(f"No vehicles key found in response data for endpoint: {endpoint}")
vehicle_ids= [vehicle['id'] for vehicle in response_data['vehicles']]

response_data =[]
for vehicle_id in vehicle_ids:
    endpoint = f"/video/vehicles/{vehicle_id}"
    vehicle_data = lytx_get_repoonse_from_event_api(endpoint)
    response_data.append(vehicle_data)

data_dict = json.dumps(response_data)
rdd = spark.sparkContext.parallelize([data_dict])
spark_df = spark.read.json(rdd) 
current_time = F.current_timestamp()
df = spark_df.withColumn("insert_time", current_time).withColumn("update_time", current_time)
if "id" in df.columns:
    primary_key = "id"
else:
    raise Exception(f"No id column found in data schema for enppoint: {endpoint}")
table_name = f"bronze.lytx_video_vehicles_vehicleId_table"
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


#initial limit and page for pagination
limit = 100
page = 1
all_vechicles=[]

#defining the table name and endpoint
table_name = f"bronze.lytx_vehicles_vehicle_id"
endpoint = f"/vehicles/all?limit={limit}&includeSubgroups=true"

#call pagination function to get all the vehicle data
all_vechicles = get_lytx_paging_data(endpoint, limit, page)
#convert to spark dataframe
df = spark.createDataFrame(all_vechicles)

#get current time stamp and add the columns tg_updated and tg_inserted to the dataframe
current_time = F.current_timestamp()
df = df.withColumn("tg_inserted", current_time).withColumn("tg_updated", current_time)

#check for vehicle id duplicates
dup_check_df = df.groupBy("id").count().filter(F.col("count") > 1)
dup_check_df.show(truncate=False)

#repartion and sort the dataframe by vehicle id and drop the duplicates
df_partitioned = df.repartition('id')
df_sorted = df_partitioned.sortWithinPartitions('id',F.desc('lastConnected'))
df = df_sorted.dropDuplicates(subset=['id'])

#perform the upsert operation(insert or update) to the table using the upsert_data function
upsert_data(df,table_name,current_time,primary_key='id',endpoint=None)



#initial limit and page for pagination
limit = 100
page = 1
all_vechicles=[]

#defining the table name and endpoint
table_name = f"bronze.lytx_video_vehicles_vehicleId"
endpoint = f"/video/vehicles?PageSize={limit}"

#call pagination function to get all the vehicle data
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






def upsert_data(df,table_name,current_time,complex_columns=None,primary_key=None,endpoint=None):
    #function perfoming an upsert
    
    #setting the value of [] to an empty list if not provided
    complex_columns = complex_columns or []

    #Check if 'id' exist in dataframe column and assign it as primary key
    #if not present it raises an exception
    if "id" in df.columns:
        primary_key = "id"
    else:
        raise Exception(f"No id column found in data schema for enppoint: {endpoint}")

    if spark.catalog.tableExists(table_name):# check if table exists
        print(f"Table {table_name} exists. Performing upsert (merge)...")
        delta_table = DeltaTable.forName(spark, table_name)  # Reference the Delta table in the database

        #performing merge operation between existing data and new data
        delta_table.alias("existing_data") \
            .merge(df.alias("new_data"), 
                F.expr(f"new_data.{primary_key} = existing_data.{primary_key}")
            ) \
         #when data matched update existing data if there are any changes in non_complex columns and columns that are not #primary keys
            .whenMatchedUpdate(
                condition=" OR ".join([
                    f"existing_data.{col} != new_data.{col}" for col in df.columns 
                    if col not in complex_columns and col != primary_key and col != "tg_inserted"
                ]),#set tg_updated to current timestamp and update other columns from new data
                set={
                    "tg_updated": current_time, 
                     **{col: F.col(f"new_data.{col}") for col in df.columns if col != primary_key and col != "tg_inserted"}}) \
                         
        #insert new data if no match is found
            .whenNotMatchedInsert(values={**{col: F.col(f"new_data.{col}") for col in df.columns}}) \
            .execute()
        print(f"Upsert (merge) completed successfully for {table_name}.")
    #create a new table if table does not exists
    else:
        print(f"Table {table_name} does not exist. Creating new table...")
        df.write.format("delta").mode("overwrite").saveAsTable(table_name)
        print(f"Table {table_name} created successfully with new data.")





++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
ddef get_lytx_paging_data(endpoint, page, limit, start_date=None, end_date=None):
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




    # def get_lytx_paging_data(endpoint, page, limit):
#     '''
#     This function handles the pagination for the lytx api for /video/vehocles and /vehicles/all endpoints
#     '''
#     all_vehicles = []
#     while True:
#         #initiating endpoints for pagination
#         if "/vehicles/all" in endpoint:
#             page_endpoint = f"/vehicles/all?limit={limit}&page={page}&includeSubgroups=true"    
#         elif "/video/vehicles" in endpoint:
#             page_endpoint = f"/video/vehicles?PageNumber={page}&PageSize={limit}"
#         else:
#             # raise Exception(f"Invalid endpoint: {endpoint}")
#         response = lytx_get_repoonse_from_event_api(page_endpoint)
        
#         if response is None or response.status_code ==204: #response is empty or status code is 204 stop paging
#             print(f" No Content on page {page}, Stopping Pagination")
#             break

#         response_data = response.json()

#         if response.status_code == 200 and 'vehicles' in response_data:
#             vehicles = response_data['vehicles']
#             print(f"Processing page{page}, with {len(vehicles)} records")
#             all_vehicles.extend(vehicles) 
#         else:
#             break
#         page +=1 #move to next page
#     print("Pagination completed")
#     return all_vehicles
