def get_item_ids_for_all_groups(access_token, group_ids, api_name, id_key):
    """
    Retrieves all Specific item id's (e.g dataset,dataflow) for each group id
    and creates a dictionatory that maps  group IDs to a list of item IDs ( e.g Dataset and Dataflow).
    """

    print(f"working on get_item_ids_for_all_groups")

    
    all_items_id = {}
    all_groups_ids = get_all_groups_ids(access_token)[0]
    for group_id in all_groups_ids:
        # print(f"fetching {api_name}  IDs for group ID: {group_id}")
        endpoint = f"groups/{group_id}/{api_name}"
        items = call_powerbi_api(access_token, endpoint)[0]

        # print(items)
        if not isinstance(items, list):
            raise Exception("API response must be a list, but got type {type(items)} for group id {group_id}")
        items_ids = [item.get(id_key) for item in items]
        all_items_id[group_id] = items_ids
    return all_items_id

get_item_ids_for_all_groups(access_token,group_ids,'dataflows', 'objectId')



def get_all_object_id_for_each_object_type(access_token, object_type, key_id ):
    """
    Get object ids for each object type in Power BI api
    """
    print(f"Working on get_all_object_id_for_each_object_type")

    
    all_groups_ids = group_ids[0]
    powerbi_object_Ids = []
    response_json = []
    headers ={ 'Authorization': f'Bearer {access_token}',
                'Content-Type': 'application/json'
        }
    base_url = 'https://api.powerbi.com/v1.0/myorg/'

    for group_id in all_groups_ids:
        url = base_url + f"/groups/{group_id}/{object_type}"
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            # print("success")
            items = response.json().get('value', [])
            response_json.append(response.json())
            for item in items:
                if key_id in item:
                    powerbi_object_Ids.append(item[key_id])
        else:
            raise Exception(f"Request failed with status {response.status_code},Response: {response.text}")
    
    return response_json

get_all_object_id_for_each_object_type(access_token, 'datasets', 'id')



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
                # print(response)
                if response.status_code == 200:
                    # print("Success")
                    if sub_api_endpoint == "refreshSchedule": #sub_api_endpoint is refreshSchedule the process since the response is a json object
                        items = response.json()
                        items[obj_id] = object_id
                        all_data.append(items)
                        all_data.append(response.json())
                    else:
                        # items = response.get('value', [])
                        items = response.json()
                        items = items.get('value', [])
                        # print(items)
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
       
    return all_data

get_object_type_items(access_token, 'dataflows', 'objectId', 'transactions')       



def create_dataframe_and_update_or_merge_table(access_token, table_name, primary_key, api_flag=None, object_type=None, key_id=None, sub_api_endpoint=None):

    """
    create Dataframe from json data and update or merge the data to delta table
    api_flag (e.g groups)
    """

    #Fetch the current timetamp
    timestamp = dt.utcnow().isoformat()
    

    if object_type is not None and sub_api_endpoint is None:
        json_object = get_all_object_id_for_each_object_type(access_token, object_type, key_id)
        # print(json_object)
    
    elif api_flag is not None:
        json_object = group_ids[1]
        # print(json_object)
        
    elif sub_api_endpoint is not None and object_type is not None and api_flag is None:
        json_object = get_object_type_items(access_token, object_type, key_id, sub_api_endpoint)
        print(json_object)
        
        
    
    if json_object:
        json_string = json.dumps(json_object)
        json_rdd = spark.sparkContext.parallelize([json_string])
        spark_df = spark.read.option("multiLine", True).json(json_rdd)
    # # primary_key = spark_df[primary_key]

        all_data_df = spark_df
        all_data_df.show()
