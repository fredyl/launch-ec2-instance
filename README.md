def get_all_object_id_for_each_object_type(access_token, object_type, key_id ):
    access_token = access_token
    
    group_ids = get_all_groups_ids(access_token)[0]
    powerbi_object_Ids = []
    response_json = []

    for group_id in group_ids:
        url = base_url + f"/groups/{group_id}/{object_type}"
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            items = response.json().get('value', [])
            response_json.append(response.json())
            for item in items:
                if key_id in item:
                    powerbi_object_Ids.append(item[key_id])
        else:
            raise Exception(f"Request failed with status {response.status_code},Response: {response.text}")
    
    return response_json, powerbi_object_Ids


def get_object_type_items(access_token, object_type, key_id, sub_api_endpoint):
    access_token = access_token
    group_ids = get_all_groups_ids(access_token)[0]
    object_ids = get_all_object_id_for_each_object_type(access_token, object_type, key_id)[1]

    items_llist = []
    for group_id in group_ids:
        for object_id in object_ids:
            url = base_url + f"/groups/{group_id}/{object_type}/{object_id}/{sub_api_endpoint}"
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                items = response.json().get('value', [])
                # print(json.dumps(items, indent=4))
                items_llist.extend(items)
        return items_llist


    def get_object_type_items(access_token, object_type, key_id, sub_api_endpoint):
    access_token = access_token
    group_ids = get_all_groups_ids(access_token)[0]
    object_ids = get_all_object_id_for_each_object_type(access_token, object_type, key_id)[1]

    items_llist = []
    for group_id in group_ids:
        for object_id in object_ids:
            url = base_url + f"/groups/{group_id}/{object_type}/{object_id}/{sub_api_endpoint}"
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                items = response.json().get('value', [])
                # print(json.dumps(items, indent=4))
                items_llist.extend(items)
        return items_llist



def create_dataframe_and_update_or_merge_table(access_token, table_name, primary_key, api_flag=None, object_type=None, key_id=None, sub_api_endpoint=None):
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

def create_dataframe_for_groups_json_data(access_token):
    access_token = access_token
    json_object = get_all_groups_ids(access_token)[1]
    json_string = json.dumps(json_object)
    
    json_rdd = spark.sparkContext.parallelize([json_string])
