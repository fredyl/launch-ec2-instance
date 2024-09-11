from requests.exceptions import HTTPError

def get_object_type_items(access_token, object_type, key_id, sub_api_endpoint):
    group_ids = get_all_groups_ids(access_token)[0]
    object_ids = get_all_object_id_for_each_object_type(access_token, object_type, key_id)[1]

    items_list = []
    for group_id in group_ids:
        print(f"Processing group_id {group_id}")
        for object_id in object_ids:
            url = f"/groups/{group_id}/{object_type}/{object_id}/{sub_api_endpoint}"
            try:
                data = call_powerbi_api(access_token, url, params=None)[1]
                if isinstance (data, dict):
                    print(f"Response Data for group_id {group_id}, object_id {object_id}: {data}")
                    if sub_api_endpoint == "refreshSchedule":
                        data[key_id] = object_id
                        items_list.append(data)
                    else:
                        if "model" in data.get("datasetType", "").lower():
                            items = data.get('value', [])
                            for item in items:
                                if isinstance (item, dict):
                                    item[key_id] = object_id
                            items_list.extend(items)
                else:
                    raise Exception(f"Error for group_id {group_id}, object_id {object_id}: {data}")
            except Exception as e:
                print(f"Error for group_id {group_id}, object_id {object_id}: {e}")
                if isinstance(e,requests.exception.HTTPError):
                    response = e.response
                    print(f"HTTP Error {response.status_code}, {response.text}")

    return items_list

print(json.dumps(get_object_type_items(access_token, "datasets", "id", "refreshSchedule"), indent=4  ))


Processing group_id e26b065e-851e-4fc4-95da-4600e0f52423
Error for group_id e26b065e-851e-4fc4-95da-4600e0f52423, object_id 38a7bb90-949b-41aa-ab2b-a5ef58482fb0: Error for group_id e26b065e-851e-4fc4-95da-4600e0f52423, object_id 38a7bb90-949b-41aa-ab2b-a5ef58482fb0: <Response [200]>
AttributeError: module 'requests' has no attribute 'exception'




