def call_power_bi_api_for_all_data(access_token, data_ids_dict,item_type, api_name, id_field):
    """
    the function makes a generic API call for each item in each group.
    """
    
    all_data = []
    skipped_data = []

#Looping through each group ID and its corresponding list of item IDs
    for group_id, item_ids in data_ids_dict.items():
        if not isinstance(item_ids, list):
            raise Exception(f"API response must be a list, but got type {type(item_ids)}")
            

        for item_id in filter(None, item_ids):
            if isinstance(item_id, dict):
                item_id = item_id.get("id")
                if item_id:
                    endpoint=f"groups/{group_id}/{item_type}/{item_id}/{api_name}"
                    # print(f"calling api for {endpoint}")

                # Call API and print the response
                try: 
                    data = call_powerbi_api(access_token, endpoint)
                    print(json.dumps(data, indent=4))
                    if isinstance(data, list):
                        for item in data:
                            item[id_field] = item_id
                        all_data.extend(data)
                    elif isinstance(data, dict):
                        data[id_field] = item_id
                        all_data.append(data)
                except Exception as e:
                    print(f"Failed to retrieve data for {endpoint}: {e}")
                    skipped_data.append(item_id)
                    continue

    if skipped_data:
        print(f"Skipped data for {group_id} : {skipped_data}")
    # all_data_rdd = spark.sparkContext.parallelize   (all_data)
    # all_data_df = spark.createDataFrame(all_data)
    # all_data_df.printSchema()

    # all_data_cleaned =all_data_df.na.drop(how= "all")
    # df = spark.createDataFrame(all_data)
    display(all_data_df)
    return all_data_df
all_data_df = call_power_bi_api_for_all_data(access_token, data_ids_dict, "datasets","datasources",id_field = "id")
