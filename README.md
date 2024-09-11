def call_power_bi_api_for_all_data(access_token, data_ids_dict, api_name, item_type):
    """
    the function makes a generic API call for each item in each group.
    """
    
    all_data = []
    skipped_data = []

#Looping through each group ID and its corresponding list of item IDs
    for group_id, item_ids in data_ids_dict.items():

        if not isinstance(item_ids, list):
            raise Exception(f"API response must be a list, but got type {type(item_ids)}")

        for item_id in item_ids:
            if not isinstance(item_id, str):
                raise Exception(f"API response must be a list, but got type {type(item_id)}")

            for item_id in item_ids:
                if item_id is None:
                    print(f"Skipping item with no id for group id {group_id}")
                    continue
            
            #Creating the API endpoint for the specific group, item type, and item ID
            endpoint=f"groups/{group_id}/{item_type}/{item_id}/{api_name}"

            #Call API and print the response
            try: 
                data = call_powerbi_api(access_token, endpoint)
                if isinstance(data, list):
                    all_data.extend(data)
                elif isinstance(data, dict):
                    all_data.append(data)
            except Exception as e:
                skipped_data.append(item_id)
                continue
        

    return all_data
