 """
    create Dataframe from json data and update or merge the data to delta table
    api_flag (e.g groups)
    """

    #Fetch the current timetamp
    timestamp = dt.utcnow().isoformat()
    

    if object_type is not None and sub_api_endpoint is None:
        json_object = get_all_object_id_for_each_object_type(access_token, object_type, key_id)
        print(json_object)
    
    elif api_flag is not None:
        json_object = group_ids[1]
        # print(json_object)
        
    elif sub_api_endpoint is not None and object_type is not None and api_flag is None:
        json_object = get_object_type_items(access_token, object_type, key_id, sub_api_endpoint)
        print(json_object)
        
        
    
    json_string = json.dumps(json_object)
    json_rdd = spark.sparkContext.parallelize([json_string])
    spark_df = spark.read.option("multiLine", True).json(json_rdd)
    # # primary_key = spark_df[primary_key]

    all_data_df = spark_df
    all_data_df.show()
        

print(create_dataframe_and_update_or_merge_table(access_token=access_token, table_name="bronze.pbi_groups", primary_key=["objectId"], object_type="dataflows", sub_api_endpoint="transactions"))



[{'id': '2023-10-04T11:18:57.8471569Z@f833dbdc-9118-409a-ba9e-e9e45c6cbe5c$13645558',
  'refreshType': 'OnDemand',
  'startTime': '2023-10-04T11:18:57.927Z',
  'endTime': '2023-10-04T11:19:00.363Z',
  'status': 'Success',
  '5ed17564-31c3-48f8-9108-bbb838cac3b3': 'objectId'},
 {'id': '2024-09-15T06:00:05.7933834Z@f85f56ad-7312-4747-8b34-c819f37ceb47$28637007',
  'refreshType': 'Scheduled',
  'startTime': '2024-09-15T06:00:07.067Z',
  'endTime': '2024-09-15T06:00:17.137Z',
  'status': 'Success',
  '582456de-a84d-4dc0-ab39-01c4640e7bf5': 'objectId'},
