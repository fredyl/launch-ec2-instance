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



Error for group_id e26b065e-851e-4fc4-95da-4600e0f52423, object_id 86af50ca-db19-48d4-b25f-60cec82c0052: Error for group_id e26b065e-851e-4fc4-95da-4600e0f52423, object_id 86af50ca-db19-48d4-b25f-60cec82c0052: ({'@odata.context': 'https://wabi-us-north-central-g-primary-redirect.analysis.windows.net/v1.0/myorg/groups/e26b065e-851e-4fc4-95da-4600e0f52423/$metadata#Microsoft.PowerBI.ServiceContracts.Api.V1.RefreshSchedule', 'days': ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'], 'times': ['14:00'], 'enabled': False, 'localTimeZoneId': 'Central Standard Time', 'notifyOption': 'NoNotification'}, <Response [200]>)
Error for group_id e26b065e-851e-4fc4-95da-4600e0f52423, object_id 4b4c0836-67bc-4e91-879f-5896169b6073: Error for group_id e26b065e-851e-4fc4-95da-4600e0f52423, object_id 4b4c0836-67bc-4e91-879f-5896169b6073: ({'@odata.context': 'https://wabi-us-north-central-g-primary-redirect.analysis.windows.net/v1.0/myorg/groups/e26b065e-851e-4fc4-95da-4600e0f52423/$metadata#Microsoft.PowerBI.ServiceContracts.Api.V1.RefreshSchedule', 'days': ['Sunday', 'Wednesday'], 'times': ['14:00'], 'enabled': True, 'localTimeZoneId': 'Central Standard Time', 'notifyOption': 'NoNotification'}, <Response [200]>)


Processing group_id e26b065e-851e-4fc4-95da-4600e0f52423
Error for group_id e26b065e-851e-4fc4-95da-4600e0f52423, object_id 0f4c44ec-6ed9-4cf5-9d52-41213e68436f: Request failed with status 415,Response: {"error":{"code":"InvalidRequest","message":"Invalid dataset. This API can only be c
