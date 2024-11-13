[CANNOT_DETERMINE_TYPE] Some of types cannot be determined after inferring.
File <command-310280906947065>, line 37
     34 # print(json.dumps(clean_data, indent =2))
     35 # convert the data to dataframe, add timestamps columns and perform an upsert
     36 if clean_data:
---> 37     df = spark.createDataFrame(clean_data)
     38     current_time = current_timestamp()
     39     df = df.withColumn("tg_inserted", current_time).withColumn("tg_updated", current_time)
File /databricks/spark/python/pyspark/instrumentation_utils.py:47, in _wrap_function.<locals>.wrapper(*args, **kwargs)
     45 start = time.perf_counter()
     46 try:
---> 47     res = func(*args, **kwargs)
     48     logger.log_success(
     49         module_name, class_name, function_name, time.perf_counter() - start, signature
     50     )
     51     return res
File /databricks/spark/python/pyspark/sql/session.py:1605, in SparkSession.createDataFrame(self, data, schema, samplingRatio, verifySchema)
   1600 if has_pandas and isinstance(data, pd.DataFrame):
   1601     # Create a DataFrame from pandas DataFrame.
   1602     return super(SparkSession, self).createDataFrame(  # type: ignore[call-overload]
   1603         data, schema, samplingRatio, verifySchema
   1604     )
-> 1605 return self._create_dataframe(
   1606     data, schema, samplingRatio, verifySchema  # type: ignore[arg-type]
   1607 )
File /databricks/spark/python/pyspark/sql/session.py:1662, in SparkSession._create_dataframe(self, data, schema, samplingRatio, verifySchema)
   1660     rdd, struct = self._createFromRDD(data.map(prepare), schema, samplingRatio)
   1661 else:
-> 1662     rdd, struct = self._createFromLocal(map(prepare, data), schema)
   1663 jrdd = self._jvm.SerDeUtil.toJavaArray(rdd._to_java_object_rdd())
   1664 jdf = self._jsparkSession.applySchemaToPythonRDD(jrdd.rdd(), struct.json())
File /databricks/spark/python/pyspark/sql/session.py:1229, in SparkSession._createFromLocal(self, data, schema)
   1221 def _createFromLocal(
   1222     self, data: Iterable[Any], schema: Optional[Union[DataType, List[str]]]
   1223 ) -> Tuple["RDD[Tuple]", StructType]:
   1224     """
   1225     Create an RDD for DataFrame from a list or pandas.DataFrame, returns the RDD and schema.
   1226     This would be broken with table acl enabled as user process does not have permission to
   1227     write temp files.
   1228     """
-> 1229     internal_data, struct = self._wrap_data_schema(data, schema)
   1230     return self._sc.parallelize(internal_data), struct
File /databricks/spark/python/pyspark/sql/session.py:1196, in SparkSession._wrap_data_schema(self, data, schema)
   1193     data = list(data)
   1195 if schema is None or isinstance(schema, (list, tuple)):
-> 1196     struct = self._inferSchemaFromList(data, names=schema)
   1197     converter = _create_converter(struct)
   1198     tupled_data: Iterable[Tuple] = map(converter, data)
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
# print(json.dumps(clean_data, indent =2))
# convert the data to dataframe, add timestamps columns and perform an upsert
if clean_data:
    df = spark.createDataFrame(clean_data)
    current_time = current_timestamp()
    df = df.withColumn("tg_inserted", current_time).withColumn("tg_updated", current_time)
    upsert_data(df,table_name,current_time,complex_columns,'id',endpoint)
else:
    print("No data recieved from API")


def replace_null_values(item_list):
    return [{k: (v if v not in["null", None] else "") for k, v in item.items()} for item in item_list]




    [
  {
    "id": "4300ffff-6680-c341-2f93-60a3e15b0000",
    "customerEventId": "DDRW50132",
    "eventTriggerId": 26,
    "eventTriggerSubTypeId": 1026,
    "eventStatusId": 1,
    "recordDateUTC": "2024-11-13T22:30:47Z",
    "recordDateTZ": "CST ",
    "recordDateUTCOffset": -360,
    "downloadedDate": "2024-11-13T22:31:24Z",
    "score": 0,
    "reviewedDate": " ",
    "erSerialNumber": "QM00001357",
    "overDue": 0.0,
    "vehicleId": "9100ffff-48a9-e663-02b3-60a3e15b0000",
    "groupId": "5100ffff-60b6-e5cd-08cb-60a3e15b0000",
    "forwardMax": 0.31,
    "lateralMax": 0.41,
    "forwardThreshold": 0.0,
    "lateralThreshold": 0.0,
    "shockThreshold": 0.0,
    "speed": 7.561999999999999,
    "latitude": 39.547696,
    "longitude": -105.034098,
    "heading": 224.3,
    "driverId": "0000ffff-0000-3a00-f1c1-4fa5892e0000",
    "coachId": "00000000-0000-0000-0000-000000000000",
    "coachedDate": "9999-01-01T00:00:00Z",
    "creationDate": "2024-11-13T22:31:23.9535936Z",
    "notes": [
      {
        "id": "5f00ffff-5bec-ae6e-e3ba-60a3e15b0000",
        "body": "RSP:|The event was triggered due to the Rolling Stop signal.|",
        "creationDate": "2024-11-13T22:31:31.2348715Z",
        "authorUserId": "00000996-0000-0000-0000-000000000000",
        "firstName": "Backend",
        "lastName": "Reviewer"
      }
    ],
    "eventNotes": [],
    "coachingSessionNotes": [],
    "reviewNotes": [
      {
        "id": "5f00ffff-5bec-ae6e-e3ba-60a3e15b0000",
        "body": "RSP:|The event was triggered due to the Rolling Stop signal.|",
        "creationDate": "2024-11-13T22:31:31.2348715Z",
        "authorUserId": "00000996-0000-0000-0000-000000000000",
        "firstName": "Backend",
        "lastName": "Reviewer"
      }
    ],
    "behaviors": [],
    "objectRevision": 4,
    "revisionDate": "2024-11-13T22:58:19.9734445Z",
    "coachEmployeeNum": " ",
    "coachFirstName": "",
    "coachLastName": "",
    "driverEmployeeNum": "10173321",
    "driverFirstName": "Tyler",
    "driverLastName": "Jesko",
    "coachingOverdueDate": "9999-01-01T00:00:00Z"
  },
  {
