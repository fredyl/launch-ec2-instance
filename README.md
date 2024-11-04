[CANNOT_DETERMINE_TYPE] Some of types cannot be determined after inferring.
File <command-310280906947065>, line 40
     36 # print(json.dumps(all_events_metadata, indent = 2))
     37 
     38 # convert the data to dataframe, add timestamps columns and perform an upsert
     39 if all_events_metadata:
---> 40     df = spark.createDataFrame(all_events_metadata) 
     41     current_time = current_timestamp()
     42     df = df.withColumn("tg_inserted", current_time).withColumn("tg_updated", current_time)
File /databricks/spark/python/pyspark/sql/session.py:1076, in SparkSession._inferSchemaFromList(self, data, names)
   1061 schema = reduce(
   1062     _merge_type,
   1063     (
   (...)
   1073     ),
   1074 )
   1075 if _has_nulltype(schema):
-> 1076     raise PySparkValueError(
   1077         error_class="CANNOT_DETERMINE_TYPE",
   1078         message_parameters={},
   1079     )
   1080 return schema


#the below codes handes the video events metadata with pagination and upsert

end_date = datetime.now().isoformat(timespec='milliseconds') + 'Z'
page =1
limit = 1000
all_events_metadata=[]
complex_columns = ['behaviors','coachingSessionNotes','eventNotes','notes', 'reviewNotes'] 
table_name = "bronze.lytx_video_eventsWithMetadata"
endpoint = f"/video/safety/eventsWithMetadata?from={start_date}&to={end_date}&dateOption=lastUpdatedDate&sortDirection=desc&sortBy=lastUpdatedDate&includeSubgroups=true&limit={limit}&page={page}"




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

endpoint = endpoint



#pagination
all_events_metadata = get_lytx_paging_data(endpoint, page, limit, start_date, end_date)
# print(json.dumps(all_events_metadata, indent = 2))

# convert the data to dataframe, add timestamps columns and perform an upsert
if all_events_metadata:
    df = spark.createDataFrame(all_events_metadata) 
    current_time = current_timestamp()
    df = df.withColumn("tg_inserted", current_time).withColumn("tg_updated", current_time)
    upsert_data(df,table_name,current_time,complex_columns,'id',endpoint)
else:
    print("No data recieved from API")
