def create_or_update_table_2( data_df, table_name, primary_keys): 
    try:
        """
        creating dataframe and table from the
         url https://api.powerbi.com/v1.0/myorg/groups/{group_id}/{api_name}

        """
        #creating a dataframe 
        data_df = data_df.dropDuplicates(primary_keys)
        data_df = data_df.withColumn("unique_id", monotonically_increasing_id())
        table_exists = spark._jsparkSession.catalog().tableExists(table_name)
        
        #creating delta_table
        if table_exists:
            
            data_df.createOrReplaceTempView("temp_view")

            if isinstance(primary_keys, list):
                merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in primary_keys]) 
            else: 
                merge_condition = " AND ".join([f"target.{col} = source.{col}" for col in primary_keys])
            
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

def create_update_groups_table(access_token, groups_data, primary_keys, table_name, api_name):

    #creating dataframe and delta table from the url https://api.powerbi.com/v1.0/myorg/{api_name}"
    
    try:
        json_strings = [json.dumps(item) for item in groups_data]
        data_rdd = spark.sparkContext.parallelize(json_strings)
        data_df = spark.read.json(data_rdd)  
        table_exists = spark._jsparkSession.catalog().tableExists(table_name)
            
        if table_exists:
            data_df.createOrReplaceTempView("temp_view")
            if isinstance(primary_keys, list):
                merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in primary_keys]) 
            else: 
                merge_condition = " AND ".join([f"target.{col} = source.{col}" for col in primary_keys])
                    
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
        return data_df

    except Exception as e:
        print(f"An error occurred: {str(e)}")
