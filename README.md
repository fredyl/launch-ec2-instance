def get_object_type_items(access_token, object_type, key_id, sub_api_endpoint):
    group_ids = get_all_groups_ids(access_token)
    _, object_ids = get_all_object_ids(access_token, object_type, key_id)
    
    items_list = []
    for group_id in group_ids:
        for object_id in object_ids:
            url = f"/groups/{group_id}/{object_type}/{object_id}/{sub_api_endpoint}"
            try:
                data = call_powerbi_api(access_token, url)
                if sub_api_endpoint == "refreshSchedule":
                    data[key_id] = object_id
                    items_list.append(data)
                else:
                    items = data.get('value', [])
                    for item in items:
                        item[key_id] = object_id
                    items_list.extend(items)
            except Exception as e:
                print(f"Error for group_id {group_id}, object_id {object_id}: {e}")
    
    return items_list


Error for group_id 3b1b45fe-32cd-4bbb-afa8-005c505e6f6a, object_id de009f69-61d7-41e3-a8e9-371567072621: Request failed with status 403,Response: {"error":{"code":"Unauthorized","message":"User is not authorized"}}
