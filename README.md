def call_power_bi_api_for_all_data(access_token, items_dict, api_name, item_type):
    """
    the function makes a generic API call for each item in each group.
    """

    if not isinstance(items_dict, dict):
        raise Exception("ids_dict must be a dictionary, but got type {type(items_dict)}")

#Looping through each group ID and its corresponding list of item IDs
    for group_id, item_ids in items_dict.items():
        print(f"processing group id : {group_id}")
        print(f"Type of item_ids: {type(item_ids)}")
        if not isinstance(item_ids, list):
            raise Exception("API response must be a list, but got type {type(items)}")


        for item_id in item_ids:
            print(f"Calling API for item ID: {item_ids}")
            if not isinstance(item_ids, str):
                raise Exception("API response must be a list, but got type {type(items)}")
            
            #Creating the API endpoint for the specific group, item type, and item ID
            endpoint=f"groups/{group_id}/{item_type}/{item_id}/{api_name}"
            print(F"print api endpoint {endpoint}")


            #Call API and print the response
            data = call_powerbi_api(access_token, endpoint)
            print(json.dumps(data, indent=2))

