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
    # spark_df = spark.read.json(json_rdd)

    #Add LastModified column to the dataframe
    spark_df = spark_df.withColumn("LastModified", lit(timestamp))
    
    if spark.catalog.tableExists(table_name):
        table_columns = [col.name for col in spark.table(table_name).schema]
        spark_df = spark_df.withColumn("InsertTime", lit(timestamp))

        print(f"Table {table_name} exists, updating")
        spark_df.createOrReplaceTempView('new_data')
        
        #Get column names for updating and inserting, only use existing columns
        # columns = [col for col in spark_df.columns if col in table_columns]
        update_columns = ", ".join([f"t.{col} = n.{col}" for col in table_columns]) # create the update column column mapping

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
    else: 
        print(f"Table {table_name} does not exist. Creating a new table.")

        spark_df = spark_df.withColumn("InsertTime", lit(timestamp))
        spark_df.write.format("delta").saveAsTable(table_name)
