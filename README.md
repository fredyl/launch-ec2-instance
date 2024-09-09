def create_dataframe_and_update_or_merge_table(access_token, table_name, primary_key, api_flag=None, object_type=None, key_id=None, sub_api_endpoint=None):

    """
    create Dataframe from json data and update or merge the data to delta table
    api_flag (e.g groups)
    """

    access_token = access_token

    if api_flag is not None:
        json_object = get_all_groups_ids(access_token)[1]
        json_string = json.dumps(json_object)
        json_rdd = spark.sparkContext.parallelize([json_string])
        spark_df = spark.read.option("multiLine", True).json(json_rdd)
        primary_key = spark_df[primary_key]

    else:
        json_object = get_object_type_items(access_token, object_type, key_id, sub_api_endpoint)
        json_string = json.dumps(json_object)
        json_rdd = spark.sparkContext.parallelize([json_string])
        spark_df = spark.read.option("multiLine", True).json(json_rdd)
        primary_key = spark_df[primary_key]

        all_data_df = spark_df
    if spark.catalog.tableExists(table_name):
        print(f"Table {table_name} exists, updating")
        existing_df = spark.table(table_name)
        all_data_df.createOrReplaceTempView('new_data')
        
        merge_condition = " AND ".join([f"t.{col} = n.{col}" for col in primary_key])

        merge_query = f"""
        MERGE INTO {table_name} AS t
        USING new_data AS n
        ON {merge_condition}
        WHEN MATCHED THEN 
        UPDATE SET *
        WHEN NOT MATCHED
         THEN INSERT *
        """
    else: 
        print(f"Table {table_name} does not exist. Creating a new table.")
        spark_df.write.format("delta").saveAsTable(table_name)



def get_object_type_items(access_token, object_type, key_id, sub_api_endpoint):
    """
    retrieves items from object_type (e.g. datasets, reports, dataflows) based on the
    object_id's
    """
 
    access_token = access_token
    group_ids = get_all_groups_ids(access_token)[0]
    object_ids = get_all_object_id_for_each_object_type(access_token, object_type, key_id)[1]

    items_llist = []
   
    for group_id in group_ids:
        for object_id in object_ids:
            url = base_url + f"/groups/{group_id}/{object_type}/{object_id}/{sub_api_endpoint}"
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                if sub_api_endpoint == "refreshSchedule":#
                    items = response.json()
                    items['key_id'] = object_id
                    items_llist.append(items)
                else:
                    items = response.json().get('value', [])
                    for item in items:
                        item['key_id'] = object_id
                    items_llist.extend(items)
        return items_llist
    
    
    
