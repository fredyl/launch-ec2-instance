def get_Item_ids_for_all_group_json(access_token, group_ids, api_name):

    all_items_json = {}
    for group_id in group_ids:
        # print(f"Fetching full JSON data for {api_name} in group ID: {group_id}") 
        endpoint = f"groups/{group_id}/{api_name}" 
        headers = {"Authorization": f"Bearer {access_token}"} 
        response = requests.get(f"https://api.powerbi.com/v1.0/myorg/{endpoint}", headers=headers)

        if response.status_code == 200:
            all_items_json[group_id] = response.json().get('value', [])
        else:
            raise Exception(f"Failed to retrieve data for group {group_id}: {response.status_code}, {response.text}")
    # print(json.dumps(all_items_json, indent=2))
    return all_items_json
