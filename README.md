def get_object_type_items(access_token, object_type, object_id, sub_api_endpoint):
    """
    Retrieves items from object_type (e.g. datasets, reports, dataflows) based on the object_id.
    """
    groups_ids = get_all_groups_ids(access_token)
    group_id_and_object_id_dict = get_item_ids_for_all_groups(access_token, groups_ids, object_type, object_id)
    all_data = []
    
      for group_id, object_ids in group_id_and_object_id_dict.items():
        print(f"Processing group: {group_id}")
        for obj_id in object_ids:
            endpoint = f"groups/{group_id}/{object_type}/{obj_id}/{sub_api_endpoint}"
            try:
                # Call the API and append the response
                response, json_data = call_powerbi_api(access_token, endpoint)
                all_data.append(json_data)
            except HTTPError as e:
                # Handle 403 errors gracefully
                if e.response.status_code == 403:
                    print(f"Unauthorized access for group {group_id}. Skipping...")
                else:
                    raise  # Reraise the error for other types of HTTP errors

    return all_data

sub_api_endpoint):
    """
    retrieves items from object_type (e.g. datasets, reports, dataflows) based on the
    object_id's
    """
    groups_ids = get_all_groups_ids(access_token)
    group_id_and_ojbect_id_dict = get_item_ids_for_all_groups(access_token, groups_ids, object_type, object_id)
    all_data = []

    for group_ids, object_ids in group_id_and_ojbect_id_dict.items():
        for obj_id in object_ids:
            endpoint= f"groups/{group_ids}/{object_type}/{obj_id}/{sub_api_endpoint}"
                # print(endpoint)
            response = call_powerbi_api(access_token, endpoint)[1]
            if response == 403:
                    print(f"Skipping item user does not have access to {group_ids}")
                    continue
            else:
                if response == 200:
                    response = call_powerbi_api(access_token, endpoint)[0]
                    all_data.append(response)
                    # print(response)
                
    return all_data

Exception: Request failed with status 403,Response: {"error":{"code":"Unauthorized","message":"User is not authorized"}}
File <command-3783901938368400>, line 53
     24     return all_data
     28     # for group_id, item_ids in group_id_and_ojbect_id_dict.items():
     29     #     # print(group_id, item_ids)
     30     #     if not isinstance(item_ids, list):
   (...)
     50     #             # skipped_data.append(item_id)
     51     #             continue
---> 53 get_object_type_items(access_token, 'dataflows', 'objectId', 'datasources')
File <command-3783901938368400>, line 14, in get_object_type_items(access_token, object_type, object_id, sub_api_endpoint)
     12 endpoint= f"groups/{group_ids}/{object_type}/{obj_id}/{sub_api_endpoint}"
     13     # print(endpoint)
---> 14 response = call_powerbi_api(access_token, endpoint)[1]
     15 if response == 403:
     16         print(f"Skipping item user does not have access to {group_ids}")
File <command-3783901938368397>, line 22, in call_powerbi_api(access_token, endpoint, params)
     20     time.sleep(retry_after)
     21 else:
---> 22     raise Exception(f"Request failed with status {response.status_code},Response: {response.text}")
