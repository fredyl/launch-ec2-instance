def get_all_object_id_for_each_object_type(access_token, object_type, key_id ):
    """
    Get object ids for each object type in Power BI api
    """
     
    group_ids = all_group_ids[0]
    powerbi_object_Ids = []
    response_json = []

    for group_id in group_ids:
        url = base_url + f"/groups/{group_id}/{object_type}"
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            items = response.json().get('value', [])
            response_json.append(response.json())
            for item in items:
                if key_id in item:
                    powerbi_object_Ids.append(item[key_id])
        else:
            raise Exception(f"Request failed with status {response.status_code},Response: {response.text}")
    
    return response_json, powerbi_object_Ids


def call_power_bi_api_for_all_data(access_token, data_ids_dict, api_name, item_type):
    """
    the function makes a generic API call for each item in each group.
    """
    
    all_data = []
    skipped_data = []

#Looping through each group ID and its corresponding list of item IDs
    for group_id, item_ids in data_ids_dict.items():

        if not isinstance(item_ids, list):
            raise Exception(f"API response must be a list, but got type {type(item_ids)}")

        for item_id in item_ids:
            if not isinstance(item_id, str):
                raise Exception(f"API response must be a list, but got type {type(item_id)}")

            for item_id in item_ids:
                if item_id is None:
                    print(f"Skipping item with no id for group id {group_id}")
                    continue
            
            #Creating the API endpoint for the specific group, item type, and item ID
            endpoint=f"groups/{group_id}/{item_type}/{item_id}/{api_name}"

            #Call API and print the response
            try: 
                data = call_powerbi_api(access_token, endpoint)
                if isinstance(data, list):
                    all_data.extend(data)
                elif isinstance(data, dict):
                    all_data.append(data)
            except Exception as e:
                skipped_data.append(item_id)
                continue
        

    return all_data




def get_object_type_items(access_token, object_type, key_id, sub_api_endpoint):
    """
    retrieves items from object_type (e.g. datasets, reports, dataflows) based on the
    object_id's
    """
    group_ids = all_group_ids[0]
    object_ids = get_all_object_id_for_each_object_type(access_token, object_type, key_id)[1]

    items_llist = []
   
    for group_id in group_ids:
        print(f"Processing group_id {group_id}")
        for object_id in object_ids:
            url = base_url + f"/groups/{group_id}/{object_type}/{object_id}/{sub_api_endpoint}"
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                if sub_api_endpoint == "refreshSchedule": #sub_api_endpoint is refreshSchedule the process since the response is a json object
                    items = response.json()
                    items[key_id] = object_id
                    items_llist.append(items)
                else:
                    items = response.json().get('value', [])
                    for item in items:
                        item[key_id] = object_id
                    items_llist.extend(items)
    return items_llist
