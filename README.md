is the below correct 


    if object_type is not None and sub_api_endpoint is None:
        json_object = get_all_object_id_for_each_object_type(access_token, object_type, key_id)
        print(json_object)
    
    elif api_flag is not None:
        json_object = group_ids[1]
        # print(json_object)
        
    elif sub_api_endpoint is not None and object_type is not None and api_flag is None:
        json_object = get_object_type_items(access_token, object_type, key_id, sub_api_endpoint)
        print(json_object)
        

print(create_dataframe_and_update_or_merge_table(access_token=access_token, table_name="bronze.pbi_groups", primary_key=["objectId"], object_type="dataflows", sub_api_endpoint="transactions"))
