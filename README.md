"""
    create Dataframe from json data and update or merge the data to delta table
    api_flag (e.g groups)
    """
    print("Running create_dataframe_and_update_or_merge_table")

    
    timestamp = dt.utcnow().isoformat() #Fetch the current timetamp

    if api_flag is not None:
        json_object = group_ids[1]
        json_string = json.dumps(json_object)
        json_rdd = spark.sparkContext.parallelize([json_string])
        spark_df = spark.read.option("multiLine", True).json(json_rdd)
        primary_key = spark_df[primary_key]
        
    elif object_type is not None and api_flag is None and sub_api_endpoint is None:
        json_object = get_all_object_id_for_each_object_type(access_token, object_type, key_id)[0]
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
    spark_df = spark_df.withColumn("LastModified", lit(timestamp))

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
