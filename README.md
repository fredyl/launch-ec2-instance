def get_object_type_items(access_token, object_type, object_id, sub_api_endpoint):
    """
    retrieves items from object_type (e.g. datasets, reports, dataflows) based on the
    object_id's
    """
    all_groups_ids = get_all_groups_ids(access_token)
    group_id_and_object_id_dict = get_item_ids_for_all_groups(access_token, all_groups_ids, object_type, object_id)
    all_data = []
    headers ={ 'Authorization': f'Bearer {access_token}',
                'Content-Type': 'application/json'
        }

    for group_id, object_ids in group_id_and_object_id_dict.items():
        if not object_ids:
            print(f"Skipping group {group_id} as it has no {object_type}s")
            continue
        for obj_id in object_ids:
            endpoint= f"/groups/{group_id}/{object_type}/{obj_id}/{sub_api_endpoint}"
            base_url = 'https://api.powerbi.com/v1.0/myorg/'
            url = base_url + endpoint
    
            try:
                response = requests.get(url, headers=headers)
                if response.status_code == 200:
                    print("Success")
                    if sub_api_endpoint == "refreshSchedule": #sub_api_endpoint is refreshSchedule the process since the response is a json object
                        items = response.json()
                        items[obj_id] = object_id
                        all_data.append(items)
                        all_data.append(response.json())
                    else:
                        items = response.get('value', [])
                        for item in items:
                            item[obj_id] = object_id
                        all_data.extend(items)
                if response.status_code == 403:
                    print(f"Skipping item user does not have access to {group_id}")
                    continue
                else:
                    if response.status_code == 403:
                        print(f"Skipping item user does not have access to {group_id}")
                        continue
            except Exception as e:
                print(f"unexpected error {e} for group {group_id}")
            # else:
            #     print(f"unexpected error {response.status_code } for group {group_id}")
                
    return all_datadef create_dataframe_and_update_or_merge_table(access_token, table_name, primary_key, api_flag=None, object_type=None, key_id=None, sub_api_endpoint=None):

    """
    create Dataframe from json data and update or merge the data to delta table
    api_flag (e.g groups)
    """

    #Fetch the current timetamp
    timestamp = dt.utcnow().isoformat()
    

    if object_type is not None:
        json_object = get_all_object_id_for_each_object_type(access_token, object_type, key_id)
    
    elif api_flag is not None:
        json_object = group_ids[1]
        # print(json_object)
        
    elif sub_api_endpoint is not None and object_type is not None:
        json_object = get_object_type_items(access_token, object_type, key_id, sub_api_endpoint)
        print(json_object)
        
    
    # json_string = json.dumps(json_object)
    # json_rdd = spark.sparkContext.parallelize([json_string])
    # spark_df = spark.read.option("multiLine", True).json(json_rdd)
    # # primary_key = spark_df[primary_key]

    # all_data_df = spark_df

    # #Add LastModified column to the dataframe
    # spark_df = spark_df.withColumn("LastModified", lit(timestamp))
    
    # if spark.catalog.tableExists(table_name):
    #     # current_time = dt.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    #     # spark_df = spark_df.withColumn("time", lit(current_time))

    #     print(f"Table {table_name} exists, updating")
    #     existing_df = spark.table(table_name)
    #     all_data_df.createOrReplaceTempView('new_data')
        
    #     merge_condition = " AND ".join([f"t.{col} = n.{col}" for col in primary_key])

    #     merge_query = f"""
    #     MERGE INTO {table_name} AS t
    #     USING new_data AS n
    #     ON {merge_condition}
    #     WHEN MATCHED THEN 
    #     UPDATE SET *
    #     WHEN NOT MATCHED
    #      THEN INSERT *
    #     """
    # else: 
    #     print(f"Table {table_name} does not exist. Creating a new table.")

    #     current_time = dt.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    #     spark_df = spark_df.withColumn("insertDatetime", lit(current_time))
    #     spark_df.write.format("delta").saveAsTable(table_name)

print(create_dataframe_and_update_or_merge_table(access_token=access_token, table_name="bronze.pbi_groups", primary_key=["objectId"], object_type="dataflows", sub_api_endpoint="datasources"))
