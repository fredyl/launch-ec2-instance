def get_object_type_items(access_token, object_type, object_id, sub_api_endpoint):
    """
    retrieves items from object_type (e.g. datasets, reports, dataflows) based on the
    object_id's
    """
    all_groups_ids = get_all_groups_ids(access_token)
    group_id_and_object_id_dict = get_item_ids_for_all_groups(access_token, all_groups_ids, object_type, object_id)
    all_data = []
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
                if response.status_code == 200:
                    print("Success")
                    if sub_api_endpoint == "refreshSchedule": #sub_api_endpoint is refreshSchedule the process since the response is a json object
                        items = response.json()
                        items[obj_id] = object_id
                        all_data.append(items)
                        all_data.append(response.json())
                    else:
                        items = response.get('value', [])
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
            # else:
            #     print(f"unexpected error {response.status_code } for group {group_id}")
                
    return all_data



DataFrame[_corrupt_record: string]


[[ {"id": "76588501-bd29-439c-b522-fc39414c4eaa", "isReadOnly": false, "isOnDedicatedCapacity": true, "capacityId": "45A20026-742A-4E9B-9FF8-319C09AD7602", "defaultDatasetStorageFormat": "Small", "type": "Workspace", "name": "Administration"}, {"id": "6ccf1a1b-877e-458c-bf1c-aabf906e52d5", "isReadOnly": false, "isOnDedicatedCapacity": true, "capacityId": "700315D0-91B1-461E-A970-68E2D9C08AC9", "defaultDatasetStorageFormat": "Small", "type": "Workspace", "name": "Microsoft Fabric Capacity Metrics 9/5/2024 4:09:52 PM"}, {"id": "e22480f3-a44b-458f-ba98-5017e044d744", "isReadOnly": false, "isOnDedicatedCapacity": true, "capacityId": "700315D0-91B1-461E-A970-68E2D9C08AC9", "defaultDatasetStorageFormat": "Small", "type": "Workspace", "name": "Fabric Chargeback Reporting 12/27/2023 7:30:55 AM"}, {"id": "485c3f61-5ecd-4bc1-a96c-cafb637cdb8c", "isReadOnly": false, "isOnDedicatedCapacity": true, "capacityId": "700315D0-91B1-461E-A970-68E2D9C08AC9", "defaultDatasetStorageFormat": "Small", "type": "Workspace", "name": "_DataHub"}, {"id": "f014186c-db9b-4c49-969b-96ad307adecf", "isReadOnly": false, "isOnDedicatedCapacity": true, "capacityId": "45A20026-742A-4E9B-9FF8-319C09AD7602", "defaultDatasetStorageFormat": "Small", "type": "Workspace", "name": "Dev"}]]
    
