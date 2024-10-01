[[CANNOT_INFER_TYPE_FOR_FIELD] Unable to infer the type of the field `devices`.](https://lytx-api.prod5.ph.lytx.com/video/safety/eventsWithMetadata?from=2022-10-31T15%3A04%3A30.000Z&to=2024-10-01T15%3A04%3A30.777Z&dateOption=lastUpdatedDate&sortDirection=desc&sortBy=lastUpdatedDate&includeSubgroups=true&limit=1000)

/video/vehicles?PageNumber={page}&PageSize={limit}

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
table_name = f"bronze.lytx_video_vehicles_vehicleId"
if spark.catalog.tableExists(table_name):
    print(f"Table {table_name} exists. Performing upsert (merge)...")
    delta_table = DeltaTable.forName(spark, table_name)  # Reference the Delta table in the database
    delta_table.alias("existing_data") \
        .merge(df.alias("new_data"), F.expr(f"new_data.{primary_key} = existing_data.{primary_key}")) \
        .whenMatchedUpdate(set={
            "update_time": current_time, 
            **{col: F.col(f"new_data.{col}") for col in df.columns if col != primary_key and col != "insert_time"}
        }) \
        .whenNotMatchedInsert(values={
            **{col: F.col(f"new_data.{col}") for col in df.columns}
        }) \
        .execute()
    print(f"Upsert (merge) completed successfully for {table_name}.")



    getting data for page:1
[
  {
    "id": 5000175793,
    "groupId": "5100ffff-60b6-e5cd-0cdb-60a3e15b0000",
    "name": "01305",
    "status": 16,
    "isWaking": false,
    "wakeable": false,
    "lastCommunication": "2024-10-01T16:56:05.6005533Z",
    "devices": [
      {
        "id": 5000183741,
        "serialNumber": "QM40851679",
        "lastCommunication": "2024-10-01T16:56:05.6005749Z",
        "onlineStatus": 16,
        "views": [
          {
            "id": 5000414052,
            "name": "FORWARD",
            "label": "Outside"
          },
          {
            "id": 5000414053,
            "name": "REAR",
            "label": "Inside"
          }
        ],
        "capabilities": [],
        "roleId": 1,
        "supportedCommands": [
          "checkinv1",
          "clipdatav1",
          "datareqv1",
          "filerequestv1",
          "ftladdv1",
          "ftllistv1",
          "ftlremovev1",
          "getsyslogv1",
          "labv1",
          "locationv1",
          "moduleremovev1",
          "moduleupdatev1",
          "performupdatev1",
          "pingv1",
          "propertiesv1",
          "rawcamv1",
          "requestsettingsreportv1",
          "requestversionsv1",
          "restartcanv1",
          "settingsv1",
          "snapshotv2",
          "statev1",
          "streamdatav1",
          "streamresetv1",
          "streamvideov1",
          "timelinev1",
          "updateavailablev1",
          "videostatev1"
        ],
        "hardwarePlatform": "SF400"
      }
    ],
else:
    print(f"Table {table_name} does not exist. Creating new table...")
    df.write.format("delta").mode("overwrite").saveAsTable(table_name)
    print(f"Table {table_name} created successfully with new data.")
