def get_Item_ids_for_all_groups(access_token, group_ids, api_name, id_key):
    """
    Retrieves all Specific item id's (e.g dataset,dataflow) for each group id
    and creates a dictionatory that maps  group IDs to a list of item IDs ( e.g Dataset and Dataflow).
    """
    all_items_id = {}

    for group_id in group_ids:
        # print(f"fetching {api_name}  IDs for group ID: {group_id}")
        endpoint = f"groups/{group_id}/{api_name}"
        items = call_powerbi_api(access_token, endpoint)

        if not isinstance(items, list):
            raise Exception("API response must be a list, but got type {type(items)} for group id {group_id}")

        #Extract the item IDs and store in a dictionary
        items_ids = [item.get(id_key) for item in items]

        all_items_id[group_id] = items_ids

    
    # print(json.dumps(all_items_id, indent=2))
    return all_items_id

get_Item_ids_for_all_groups(access_token, get_all_groups_ids(access_token), 'dataflows', 'objectId')
