DataFrame[capacityId: string, defaultDatasetStorageFormat: string, id: string, isOnDedicatedCapacity: boolean, isReadOnly: boolean, name: string, type: string]

def create_update_groups_table(access_token):
    
    try:
        groups_data = get_groups_id(access_token)
        groups_data = groups_data.get("value", [])
        grous_data_json = [json.dumps(group_data) for group_data in groups_data]
        data_rdd = spark.sparkContext.parallelize(grous_data_json)
        data_df = spark.read.json(data_rdd)
        display(data_df)
        return data_df
        table_exists = spark._jsparkSession.catalog().tableExists(table_name)
        
        if table_exists:
            
            data_df.createOrReplaceTempView("temp_view")

            if isinstance(primary_keys, list):
                merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in primary_keys]) 
            else: 
                merge_condition = f"target.{primary_keys} = source.{primary_keys}"
                # merge_condition = " AND ".join([f"target.{col} = source.{col}" for col in primary_key])
            
            merge_query = f"""
            MERGE INTO {table_name} AS target
            USING temp_view AS source
            ON {merge_condition}
            WHEN MATCHED THEN
                UPDATE SET *
            WHEN NOT MATCHED THEN
                INSERT *
            """

            

            spark.sql(merge_query)

        else:
            print(f"Table {table_name} does not exist. Creating a new table.")
            data_df.write.mode("overwrite").saveAsTable(table_name)

    except Exception as e:
        print(f"An error occurred: {str(e)}")
