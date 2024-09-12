def get_object_type_items(access_token, object_type, object_id, sub_api_endpoint):
    """
    retrieves items from object_type (e.g. datasets, reports, dataflows) based on the
    object_id's
    """
    all_groups_ids = get_all_groups_ids(access_token)
    group_id_and_object_id_dict = get_item_ids_for_all_groups(access_token, all_groups_ids, object_type, object_id)
    all_data = []

    for group_id, object_ids in group_id_and_object_id_dict.items():
        for obj_id in object_ids:
            endpoint= f"/groups/{group_id}/{object_type}/{obj_id}/{sub_api_endpoint}"
            print(endpoint)
            base_url = 'https://api.powerbi.com/v1.0/myorg/'
            response = requests.get(base_url + endpoint)
            print(response)
            if response.status_code == 200:
                print("Success")
                all_data.append(response.json())
            # if response.status_code == 403:
            #     print(f"Skipping item user does not have access to {group_id}")
            #     continue
            else:
                if response.status_code == 403:
                    print(f"Skipping item user does not have access to {group_id}")
                    continue
            # else:
            #     print(f"unexpected error {response.status_code } for group {group_id}")
                
    return all_data

get_object_type_items(access_token, 'datasets', 'id', 'datasources')       




 /groups/e26b065e-851e-4fc4-95da-4600e0f52423/datasets/38a7bb90-949b-41aa-ab2b-a5ef58482fb0/datasources
<Response [404]>
