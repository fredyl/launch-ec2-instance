def get_object_type_items(access_token, object_type, key_id, sub_api_endpoint):
    """
    retrieves items from object_type (e.g. datasets, reports, dataflows) based on the
    object_id's
    """
    groups_ids = all_group_ids[0]
    object_ids = get_all_object_id_for_each_object_type(access_token, object_type, key_id)[1]

    items_llist = []
    for group_id in groups_ids:
        for object_id in object_ids:
            endpoint = f"groups/{group_id}/{object_type}/{object_id}/{sub_api_endpoint}"

            status_code, json_data  = call_powerbi_api(access_token, endpoint)
            # status_code = response[0]
            if status_code == 200:
                # items = response[1].get('value', [])
                items = json_data.get('value', [])
                for item in items:
                    item[key_id] = object_id
                items_llist.extend(items)
            elif status_code == 404:
                    print(f"resource not found for url: {endpoint}. Skipping ...")
            else:
                print(f"Request failed with status {status_code}, URL: {endpoint}")
                continue
    return items_llist

print(get_object_type_items(access_token, 'datasets', 'id', 'refreshes'))


Exception: Request failed with status 415,Response: {"error":{"code":"InvalidRequest","message":"Invalid dataset. This API can only be called on a Model-based dataset"}}
File <command-2798623019031549>, line 29
     26                 continue
     27     return items_llist
---> 29 print(get_object_type_items(access_token, 'datasets', 'id', 'refreshes'))
File <command-3383306747622981>, line 16, in call_powerbi_api(access_token, endpoint, params)
     14     time.sleep(retry_after)
     15 else:
---> 16     raise Exception(f"Request failed with status {response.status_code},Response: {response.text}")
