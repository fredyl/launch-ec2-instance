def get_object_type_items(access_token, object_type, object_id, sub_api_endpoint):
    """
    retrieves items from object_type (e.g. datasets, reports, dataflows) based on the
    object_id's
    """
    # all_groups_ids = group_ids
    group_id_and_object_id_dict = get_item_ids_for_all_groups(access_token, group_ids, object_type, object_id)
    all_data = []
    # base_url = 'https://api.powerbi.com/v1.0/myorg/'
    # headers ={ 'Authorization': f'Bearer {access_token}',
    #             'Content-Type': 'application/json'
    #     }

    for group_id, object_ids in group_id_and_object_id_dict.items():
        if not object_ids:
            print(f"Skipping group {group_id} as it has no {object_type}s")
            continue
        for obj_id in object_ids:
            endpoint= f"/groups/{group_id}/{object_type}/{obj_id}/{sub_api_endpoint}"
            # base_url = 'https://api.powerbi.com/v1.0/myorg/'
            # url = base_url + endpoint
            try:
                response_data, status_code = call_powerbi_api(access_token, endpoint)
                if status_code == 200:
                    if isinstance(response_data, dict):
                        if sub_api_endpoint == "refreshSchedule": #sub_api_endpoint is refreshSchedule the process since the response is a json object
                            items = response_data
                            items['object_id'] = obj_id
                            all_data.append(response_data)
                        else:
                            items = response_data.get('value', [])
                            for item in items:
                                item['object_id'] = obj_id
                            all_data.extend(items)
                    elif isinstance(response_data, list):
                        for item in items:
                                item['object_id'] = obj_id
                        all_data.extend(response_data)    
                elif status_code == 403:
                    print(f"Skipping item user does not have access to {group_id}")
                    continue
                elif status_code == 415:
                        print(f"Invalid dataset for group {group_id}. this API can only be called on a Model-based dataset")
                        continue
            except Exception as e:
                print(f"unexpected error {e} for group {group_id}")
       
    return all_data

get_object_type_items(access_token, "datasets", "id", "refreshes")
