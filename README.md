def create_dataframe_and_update_or_merge_table(access_token, table_name, primary_key, api_flag=None, object_type=None, key_id=None, sub_api_endpoint=None):

    """
    create Dataframe from json data and update or merge the data to delta table
    api_flag (e.g groups)
    """

    #Fetch the current timetamp
    timestamp = dt.utcnow().isoformat()

    if object_type is not None and sub_api_endpoint is None:
        json_object = get_all_object_id_for_each_object_type(access_token, object_type, key_id)
    elif api_flag is not None:
        json_object = group_ids[1]
    elif sub_api_endpoint is not None and object_type is not None and api_flag is None:
        json_object = get_object_type_items(access_token, object_type, key_id, sub_api_endpoint)
  
    json_string = json.dumps(json_object)
    json_rdd = spark.sparkContext.parallelize([json_string])
    spark_df = spark.read.option("multiLine", True).json(json_rdd)
    spark_df = spark.read.json(json_rdd)
    all_data_df = spark_df

    #Add LastModified column to the dataframe
    spark_df = spark_df.withColumn("LastModified", lit(timestamp))
    
    if spark.catalog.tableExists(table_name):
        current_time = dt.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        spark_df = spark_df.withColumn("time", lit(current_time))

        print(f"Table {table_name} exists, updating")
        # existing_df = spark.table(table_name)
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

        current_time = dt.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        spark_df = spark_df.withColumn("insertDatetime", lit(current_time))
        spark_df.write.format("delta").saveAsTable(table_name)
