def get_object_type_items(access_token, object_type, key_id, sub_api_endpoint):
    """
    retrieves items from object_type (e.g. datasets, reports, dataflows) based on the
    object_id's
    """
    print("Running get_object_type_items")

    access_token = access_token
    group_ids = get_all_groups_ids(access_token)[0]
    object_ids = get_all_object_id_for_each_object_type(access_token, object_type, key_id)[1]

    items_llist = []
    if sub_api_endpoint == "refreshSchedule":
        for group_id in group_ids:
            for object_id in object_ids:
                url = base_url + f"groups/{group_id}/{object_type}/{object_id}/{sub_api_endpoint}"
                # print(url)
                response = requests.get(url, headers=headers)
                print(response.json())
                if response.status_code == 200:
                    items = response.json()
                    items_llist.append(items)
            return items_llist
    else:
        for group_id in group_ids:
            for object_id in object_ids:
                url = base_url + f"/groups/{group_id}/{object_type}/{object_id}/{sub_api_endpoint}"
                response = requests.get(url, headers=headers)
                if response.status_code == 200:
                    items = response.json().get('value', [])
                    # print(json.dumps(items, indent=4))
                    items_llist.extend(items)
            return items_llist
    
