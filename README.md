def create_dataframe(api_name, group_ids ):
    # group_ids = get_all_groups_ids_1(access_token)
    # # combined_df = None
    try:
        full_json_data = get_Item_ids_for_all_group_json(access_token, group_ids, api_name)
        data_rdd = spark.sparkContext.parallelize([json.dumps(full_json_data['value'])])
    except KeyError:
        print("The 'value' key was not found in the API response.")
            
            # data_rdd = spark.sparkContext.parallelize([json.dumps(get_json_data['value'])])
        data_df = spark.read.json(data_rdd)
        data_df.show()

    return data_df
 
        
