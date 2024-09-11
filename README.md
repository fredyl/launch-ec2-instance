def call_powerbi_api(access_token, endpoint, params=None):

    url = base_url + endpoint
    for _ in range(3):
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            data = response.json()
            return data['value'] if 'value' in data else data
        elif response.status_code == 429:
            retry_after = int(response.headers.get('Retry-After', 8))
            time.sleep(retry_after)
        else:
            raise Exception(f"Request failed with status {response.status_code},Response: {response.text}")

headers= { 'Authorization': f'Bearer {access_token}',
                'Content-Type': 'application/json'
        }
base_url = 'https://api.powerbi.com/v1.0/myorg/'


def get_all_groups_ids(access_token):
    groups_data = call_powerbi_api(access_token, 'groups')
    group_ids = [groups_data['id'] for groups_data in groups_data]
    return group_ids, groups_data

all_group_ids = get_all_groups_ids(access_token)


def get_all_object_id_for_each_object_type(access_token, object_type, key_id): 
    """ Get object ids for each object type in Power BI API. """ 
    
    group_ids = all_group_ids[0] 
    powerbi_object_Ids = [] 
    response_json = []

    for group_id in group_ids:
        url = base_url + f"/groups/{group_id}/{object_type}"
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status() 
            data = response.json()
            response_json.append(data)
            items = data.get('value', [])
            powerbi_object_Ids.extend(item[key_id] for item in items if key_id in item)
        except requests.RequestException as e:
            print(f"Request failed for group_id {group_id} with error: {e}")
        except Exception as e:

            print(f"An error occurred: {e}")

    return response_json, powerbi_object_Ids



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
    
print(get_object_type_items(access_token, "datasets", "id", "refreshSchedule"))
