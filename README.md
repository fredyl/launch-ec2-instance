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




 '97b99d50-bc67-4e69-84d3-1c78e0c7db21': ['5ed17564-31c3-48f8-9108-bbb838cac3b3'],
 '995e1915-5334-43d8-bc4b-4efb40ff9adf': [],
 'da9faf07-0f02-48a8-8796-6052339a6dd1': [],
 '2c87a876-ae5f-4ca1-a0ca-573556457626': [],
 '88a250a3-615a-4118-81f2-6ed110811a25': ['582456de-a84d-4dc0-ab39-01c4640e7bf5',
  'ed135cbf-0450-4645-96bb-e1f51c0a53b4',
