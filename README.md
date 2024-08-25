ef get_Item_ids_for_all_groups(access_token, group_ids, api_name, id_key):
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
        # print(json.dumps(items, indent=2))
        items_ids = [item.get(id_key) for item in items]
        # for item in items:
        #     item_id = item.get('id_key')
        #     if item_id is not None:
        #         items_ids.append(item_id)
        #     else:
        #         print(f"Skipping item with no id for group id {group_id}")

        # [item.get(id_key) for item in items]

        all_items_id[group_id] = items_ids

    
    print(json.dumps(all_items_id, indent=2))
    return all_items_id
    
    
