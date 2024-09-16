def get_all_groups_ids(access_token):
    groups_data = call_powerbi_api(access_token, 'groups')[0]
    group_ids = [groups_data['id'] for groups_data in groups_data]
    return group_ids , groups_data
group_ids = get_all_groups_ids(access_token)



def get_item_ids_for_all_groups(access_token, group_ids, api_name, id_key):
    """
    Retrieves all Specific item id's (e.g dataset,dataflow) for each group id
    and creates a dictionatory that maps  group IDs to a list of item IDs ( e.g Dataset and Dataflow).
    """
    print(f"working on get_item_ids_for_all_groups")

    all_items_id = {}
    all_groups_ids = group_ids[0]
    for group_id in all_groups_ids:
        endpoint = f"groups/{group_id}/{api_name}"
        items = call_powerbi_api(access_token, endpoint)[0]
        if not isinstance(items, list):
            raise Exception("API response must be a list, but got type {type(items)} for group id {group_id}")
        items_ids = [item.get(id_key) for item in items]
        all_items_id[group_id] = items_ids
    return all_items_id


def get_all_object_id_for_each_object_type(access_token, object_type, key_id ):
    """
    Get object ids for each object type in Power BI api
    """
    print(f"Working on get_all_object_id_for_each_object_type")


    all_groups_ids = group_ids[0]
    powerbi_object_Ids = []
    response_json = []

    for group_id in all_groups_ids:
        endpoint = f"/groups/{group_id}/{object_type}"
        response = call_powerbi_api(access_token, endpoint)[0]
        if isinstance(response, dict):
            items = response.get('value', [])
            response_json.extend(items)
        elif isinstance(response, list):
            response_json.extend(response)
        else:
            raise Exception(f"Response is not a dictionary or a list: {response}")
            raise Exception(f"Response is not a dictionary: {response}")

    return response_json

def get_object_type_items(access_token, object_type, object_id, sub_api_endpoint):
    """
    retrieves items from object_type (e.g. datasets, reports, dataflows) based on the
    object_id's
    """
    all_groups_ids = group_ids[0]
    group_id_and_object_id_dict = get_item_ids_for_all_groups(access_token, all_groups_ids, object_type, object_id)
    all_data = []
    base_url = 'https://api.powerbi.com/v1.0/myorg/'
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
                    if sub_api_endpoint == "refreshSchedule": #sub_api_endpoint is refreshSchedule the process since the response is a json object
                        items = response.json()
                        all_data.append(items)
                    else:
                        items = response.json()
                        items = items.get('value', [])
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
       
    return all_data

get_object_type_items(access_token, "datasets", "id", "refreshSchedule")




def get_object_type_items(access_token, object_type, object_id, sub_api_endpoint):
    """
    retrieves items from object_type (e.g. datasets, reports, dataflows) based on the
    object_id's
    """
    all_groups_ids = group_ids[0]
    group_id_and_object_id_dict = get_item_ids_for_all_groups(access_token, all_groups_ids, object_type, object_id)
    all_data = []
    base_url = 'https://api.powerbi.com/v1.0/myorg/'
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
                    if sub_api_endpoint == "refreshSchedule": #sub_api_endpoint is refreshSchedule the process since the response is a json object
                        items = response.json()
                        all_data.append(items)
                    else:
                        items = response.json()
                        items = items.get('value', [])
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
       
    return all_data

get_object_type_items(access_token, "datasets", "id", "refreshSchedule")
    
