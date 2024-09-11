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
            # print(endpoint)
            response = call_powerbi_api(access_token, endpoint)
            if response[0] == 200:
                items = response.get('value', [])
                for item in items:
                    item[key_id] = object_id
                items_llist.extend(items)
            else:
                print(f"Skipping URL: {endpoint} - Response: {response[0].status_code}")
                continue
    return items_llist
get_object_type_items(access_token, 'datasets', 'id','datasources')
