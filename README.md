
def get_object_type_items(access_token, object_type, object_id, sub_api_endpoint):
    """
    retrieves items from object_type (e.g. datasets, reports, dataflows) based on the
    object_id's
    """
    
    group_id_and_object_id_dict = get_item_ids_for_all_groups(access_token, group_ids, object_type, object_id)
    all_data = []

    for group_id, object_ids in group_id_and_object_id_dict.items():
        if not object_ids:
            print(f"Skipping group {group_id} as it has no {object_type}s")
            continue
        for obj_id in object_ids:
            endpoint= f"/groups/{group_id}/{object_type}/{obj_id}/{sub_api_endpoint}"
            try:
                # response_data, status_code = call_powerbi_api(access_token, endpoint)
                response_data= call_powerbi_api(access_token, endpoint)[0]
                status_code = call_powerbi_api(access_token, endpoint)[1]
                if status_code == 200:
                    if isinstance(response_data, dict):
                        items = [response_data] if sub_api_endpoint == 'refreshSchedule' else response_data.get('value', [])
                    elif isinstance(response_data, list):
                        items = response_data
                    else:
                        items = []
                    for item in items:
                        item[object_id] = obj_id
                    all_data.extend(items)
            except Exception as e:
                print(f"unexpected error {e} for group {group_id}")
       
    return all_data

def create_dataframe_and_update_or_merge_table(access_token, table_name, primary_key, api_flag=None, object_type=None, key_id=None, sub_api_endpoint=None):

    """
    create Dataframe from json data and update or merge the data to delta table
    api_flag (e.g groups)
    """

    #Fetch the current timetamp
    timestamp = datetime.utcnow().isoformat()

    if object_type is not None and sub_api_endpoint is None:
        json_object = get_all_object_id_for_each_object_type(access_token, object_type, key_id)
    elif api_flag is not None:
        json_object = groups_data
    elif sub_api_endpoint is not None and object_type is not None and api_flag is None:
        json_object = get_object_type_items(access_token, object_type, key_id, sub_api_endpoint)
  
    json_string = json.dumps(json_object)
    json_rdd = spark.sparkContext.parallelize([json_string])
    spark_df = spark.read.option("multiLine", True).json(json_rdd)
    spark_df = spark_df.dropDuplicates(primary_key)

    if "@odata.context" in spark_df.columns:
        spark_df = spark_df.drop("@odata.context")

    # spark_df.display()

#     #Add LastModified column to the dataframe
    spark_df = spark_df.withColumn("LastModified", to_timestamp(lit(timestamp)))
    spark_df = spark_df.withColumn("InsertTime", to_timestamp(lit(timestamp)))

    if spark.catalog.tableExists(table_name):
        table_columns = [col.name for col in spark.table(table_name).schema]
        
        print(f"Table {table_name} exists, updating")
        spark_df.createOrReplaceTempView('new_data')

        update_columns = ", ".join([f"t.{col} = n.{col}" for col in table_columns if col != "InsertTime"]) # create the update column column mapping
       
        merge_condition = " AND ".join([f"t.{col} = n.{col}" for col in primary_key])

        merge_query = f"""
        MERGE INTO {table_name} AS t
        USING new_data AS n
        ON {merge_condition}
        WHEN MATCHED THEN 
        UPDATE SET {update_columns}
        WHEN NOT MATCHED THEN 
        INSERT *
        """
        # print(merge_query)
        spark.sql(merge_query)
    else: 
        print(f"Table {table_name} does not exist. Creating a new table.")
        spark_df.write.format("delta").saveAsTable(table_name)
