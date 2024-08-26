def create_or_update_table(headers, rows, table_name,primary_key_columns):
 
    if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
        raise ValueError(f"expected rows to be a list of dicts")

    schema = StructType([StructField(header, StringType(), True) for header in headers])
    df = spark.createDataFrame([tuple(row.values()) for row in rows], schema)

    if spark._jsparkSession.catalog().tableExists(table_name):
        print(f"Table {table_name} exists, updating")

        existing_df = spark.table(table_name)
        df.createOrReplaceTempView('new_data')
        
        merge_condition = " AND ".join([f"t.{col} = n.{col}" for col in primary_key_columns])
        

        merge_query = f"""
        MERGE INTO {table_name} AS t
        USING new_data AS n
        ON {merge_condition}
        WHEN MATCHED THEN 
        UPDATE SET *
        WHEN NOT MATCHED
         THEN INSERT *
        """

        spark.sql(merge_query)

    else:
        print(f"Table {table_name} does not exist, creating")
        df.write.format("delta").saveAsTable(table_name)

    if not spark._jsparkSession.catalog().tableExists(table_name):
        raise ValueError(f"Table {table_name} could not be created")



def call_power_bi_api_for_all_data(access_token, items_dict, api_name, item_type):
    """
    the function makes a generic API call for each item in each group.
    """
    
    all_data = []
    skipped_data = []

#Looping through each group ID and its corresponding list of item IDs
    for group_id, item_ids in items_dict.items():
        print(f"processing group id : {group_id}")
        # print(f"Type of item_ids: {type(item_ids)}")
        if not isinstance(item_ids, list):
            raise Exception(f"API response must be a list, but got type {type(item_ids)}")

        for item_id in item_ids:
            print(f"Calling API for item ID: {item_id}")
            if not isinstance(item_id, str):
                raise Exception(f"API response must be a list, but got type {type(item_id)}")

            for item_id in item_ids:
                if item_id is None:
                    print(f"Skipping item with no id for group id {group_id}")
                    continue
            print(f"Calling API for item ID: {item_id}")
            if not isinstance(item_id, str):
                raise Exception(f"API response must be a list, but got type {type(item_id)}")

            
            #Creating the API endpoint for the specific group, item type, and item ID
            endpoint=f"groups/{group_id}/{item_type}/{item_id}/{api_name}"
            # print(F"print api endpoint {endpoint}")


            #Call API and print the response
            try: 
                data = call_powerbi_api(access_token, endpoint)
                if isinstance(data, list):
                    all_data.extend(data)
                elif isinstance(data, dict):
                    all_data.append(data)
            except Exception as e:
                print(f"API call failed with Error: {e}")
                skipped_data.append(item_id)
                continue
        
    if skipped_data:
        print(f"Skipped data for {group_id} : {skipped_data}")

    return all_data
    



