def get_object_type_items(access_token, object_type, key_id, sub_api_endpoint):
    group_ids = get_all_groups_ids(access_token)[0]
    object_ids = get_all_object_id_for_each_object_type(access_token, object_type, key_id)[1]


        items_list = []
            for group_id in group_ids:
                print(f'Porcessing group_id: {group_id}')
                for object_id in object_ids:
                    endpoint = f"/groups/{group_id}/{object_type}/{object_id}/{sub_api_endpoint}"
                    # print(endpoint)
                    try:
                        response = call_powerbi_api(access_token, endpoint)[1]
                        if response.status_code == 200:
                            data =response.json()
                            # if isinstance (data, dict):
                            #     print(f"Response Data for group_id {group_id}, object_id {object_id}: {data}")
                            if sub_api_endpoint == "refreshSchedule":
                                data[key_id] = object_id
                                items_list.extend(data)
                                print(items_list)
                            else:
                                items = data.get('value', [])
                                for item in items:
                                    item[key_id] = object_id
                                items_list.append(items)
                    except Exception as e:



def get_all_groups_ids(access_token):
    groups_data = call_powerbi_api(access_token, 'groups')
    group_ids = [group['id'] for group in groups_data[0]['value']]
    return group_ids, groups_data

all_group_ids = get_all_groups_ids(access_token)
                print(f"Error for group_id {group_id}, object_id {object_id}: {e}")

    return items_list

Error for group_id 3b1b45fe-32cd-4bbb-afa8-005c505e6f6a, object_id de009f69-61d7-41e3-a8e9-371567072621: Request failed with status 403,Response: {"error":{"code":"Unauthorized","message":"User is not authorized"}}
