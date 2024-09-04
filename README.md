def get_Item_ids_for_all_groups(access_token, group_ids, api_name, id_field=None, retrurn_full_json = False):
    """
    Retrieves all Specific item id's (e.g dataset,dataflow) for each group id
    and creates a dictionatory that maps  group IDs to a list of item IDs ( e.g Dataset and Dataflow).
    """
    all_items_data = {}
    
    for group_id in group_ids:
        
        endpoint = f"groups/{group_id}/{api_name}"
        items = call_powerbi_api(access_token, endpoint)

        if retrurn_full_json:
            if isinstance(items, list):
                all_items_data.extend(items) # store the json response
            elif isinstance(items, dict):
                all_items_data.append(items)
            else: 
                raise Exception(f"Expected list for group {group_id} but got {type(items)}")
        else:
            if not id_field:
                raise ValueError(f"id_field must be be provided when return_full_json is False")

            #Extract the item IDs and store in a dictionary
            if isinstance(items, list):
                items_ids = [item.get(id_field) for item in items]
                all_items_data.extend(items_ids)
            elif isinstance(items, dict):
                items_ids = items.get(id_field)
                all_items_data.append(items_ids)
            else:
                raise Exception(f"API response must be a list for group {group_id} but got {type(items)}")

    return all_items_data
