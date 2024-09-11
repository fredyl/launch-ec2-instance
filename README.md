Error for group_id 3b1b45fe-32cd-4bbb-afa8-005c505e6f6a, object_id 00f9354c-8c9a-4e5a-b9a0-9c2eb38e3797: 'tuple' object does not support item assignment


def get_object_type_items(access_token, object_type, key_id, sub_api_endpoint):
    group_ids = get_all_groups_ids(access_token)[0]
    object_ids = get_all_object_id_for_each_object_type(access_token, object_type, key_id)[1]

    items_list = []
    for group_id in group_ids:
        print(f"Processing group_id {group_id}")
        for object_id in object_ids:
            url = f"/groups/{group_id}/{object_type}/{object_id}/{sub_api_endpoint}"
            try:
                data = call_powerbi_api(access_token, url, params=None)
                if sub_api_endpoint == "refreshSchedule":
                    data[key_id] = object_id
                    items_list.append(data)
                else:
                    if "model" in data.get("datasetType", "").lower():
                        items = data.get('value', [])
                        for item in items:
                            item[key_id] = object_id
                        items_list.extend(items)
            except Exception as e:
                print(f"Error for group_id {group_id}, object_id {object_id}: {e}")

    return items_list

print(get_object_type_items(access_token, "datasets", "id", "refreshSchedule"))


 base_url = "YOUR_BASE_URL"  # Replace with your actual base URL
    group_ids = all_group_ids[0]
    powerbi_object_Ids = []
    response_json = []

    for group_id in group_ids:
        endpoint = f"/groups/{group_id}/{object_type}"
        response = call_powerbi_api(access_token, endpoint)
        if response.status_code == 200:
            items = response.json().get('value', [])
            response_json.append(response.json())
            for item in items:
                if key_id in item:
                    powerbi_object_Ids.append(item[key_id])
        else:
            raise Exception(f"Request failed with status {response.status_code}, Response: {response.text}")

    return response_json, powerbi_object_Ids


AttributeError: 'tuple' object has no attribute 'status_code'
