

create_dataframe_and_update_or_merge_table(access_token, "dev.bronze.pbi_dataflow",["objectId"], api_flag=None, object_type="dataflows")
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
    elif object_type is not None:
        json_object = get_all_object_id_for_each_object_type(access_token, object_type, key_id)[1]
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


[UNRESOLVED_COLUMN.WITHOUT_SUGGESTION] A column, variable, or function parameter with name `objectId` cannot be resolved.  SQLSTATE: 42703
File <command-4094120553390858>, line 1
----> 1 create_dataframe_and_update_or_merge_table(access_token, "dev.bronze.pbi_dataflow",["objectId"], api_flag=None, object_type="dataflows")
File /databricks/spark/python/pyspark/errors/exceptions/captured.py:261, in capture_sql_exception.<locals>.deco(*a, **kw)
    257 converted = convert_exception(e.java_exception)
    258 if not isinstance(converted, UnknownException):
    259     # Hide where the exception came from that shows a non-Pythonic
    260     # JVM exception message.
--> 261     raise converted from None
    262 else:
    263     raise
