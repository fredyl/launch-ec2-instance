def get_object_type_items(access_token, object_type, object_id, sub_api_endpoint):
    """
    retrieves items from object_type (e.g. datasets, reports, dataflows) based on the
    object_id's
    """
    all_groups_ids = get_all_groups_ids(access_token)
    group_id_and_object_id_dict = get_item_ids_for_all_groups(access_token, all_groups_ids, object_type, object_id)
    all_data = []
    headers ={ 'Authorization': f'Bearer {access_token}',
                'Content-Type': 'application/json'
        }

    for group_id, object_ids in group_id_and_object_id_dict.items():
        if not object_ids:
            print(f"Skipping group {group_id} as it has no {object_type}s")
            continue
        for obj_id in object_ids:
            endpoint= f"/groups/{group_id}/{object_type}/{obj_id}/{sub_api_endpoint}"
            base_url = 'https://api.powerbi.com/v1.0/myorg/'
            url = base_url + endpoint
    
            try:
                response = requests.get(url, headers=headers)
                if response.status_code == 200:
                    print("Success")
                    if sub_api_endpoint == "refreshSchedule": #sub_api_endpoint is refreshSchedule the process since the response is a json object
                        items = response.json()
                        items[obj_id] = object_id
                        all_data.append(items)
                        all_data.append(response.json())
                    else:
                        items = response.get('value', [])
                        for item in items:
                            item[obj_id] = object_id
                        all_data.extend(items)
                if response.status_code == 403:
                    print(f"Skipping item user does not have access to {group_id}")
                    continue
                else:
                    if response.status_code == 403:
                        print(f"Skipping item user does not have access to {group_id}")
                        continue
            except Exception as e:
                print(f"unexpected error {e} for group {group_id}")
            # else:
            #     print(f"unexpected error {response.status_code } for group {group_id}")
                
    return all_data
