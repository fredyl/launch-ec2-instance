when I use the functions statuses_api_response the data is populated as below. but then when trying to create a dataframe while using the function below process_api_data_to_delta i get _corrupt_record.
Why the dataframes comes as corrupted record and how can I fix the issue. since will need to use the dataframe to create a delta table

def statuses_api_response():
    endpoint = "/vehicles/all"   
    response_data = lytx_get_repoonse_from_event_api(endpoint)
    return response_data
    print(response_data)
statuses_api_response()


def process_api_data_to_delta(endpoint, table_name):
    # Fetch the API response
    response_data = lytx_get_repoonse_from_event_api(endpoint)
    
    # Convert JSON response to DataFrame
    if isinstance(response_data, list):
        df = spark.createDataFrame(response_data)  # Handle if the response is a list
    else:
        rdd = spark.sparkContext.parallelize([response_data])
        df = spark.read.json(rdd)
    
    # Add insert_time and update_time columns
    current_time = F.current_timestamp()
    df = df.withColumn("insert_time", current_time).withColumn("update_time", current_time)
    df.display()

    
{'totalCount': 6872,
 'vehicles': [{'id': 5000175793,
   'groupId': '5100ffff-60b6-e5cd-0cdb-60a3e15b0000',
   'name': '01305',
   'status': 16,
   'isWaking': False,
   'wakeable': False,
   'lastCommunication': '2024-09-25T15:00:13.7917521Z',
   'devices': [{'id': 5000183741,
     'serialNumber': 'QM40851679',
     'lastCommunication': '2024-09-25T15:00:13.7917678Z',
     'onlineStatus': 16,
     'views': [{'id': 5000414052, 'name': 'FORWARD', 'label': 'Outside'},
      {'id': 5000414053, 'name': 'REAR', 'label': 'Inside'}],
     'capabilities': [],
     'roleId': 1,
     'supportedCommands': ['checkinv1',
      'clipdatav1',
      'datareqv1',
      'filerequestv1',
      'ftladdv1',
      'ftllistv1',
      'ftlremovev1',
      'getsyslogv1',
      'labv1',
      'locationv1',
      'moduleremovev1',
      'moduleupdatev1',
      'performupdatev1',
      'pingv1',
      'propertiesv1',
      'rawcamv1',
      'requestsettingsreportv1',
      'requestversionsv1',
      'restartcanv1',
      'settingsv1',
      'snapshotv2',
      'statev1',
      'streamdatav1',
      'streamresetv1',
      'streamvideov1',
      'timelinev1',
      'updateavailablev1',
      'videostatev1'],
     'hardwarePlatform': 'SF400'}],
   'dcVehicleId': '9100ffff-48a9-e863-445b-60a3e15b0000'}]}
   'deviceId': '00000000-0000-0000-0000-000000000000'}],
   'totalResults':1258
