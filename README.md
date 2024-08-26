headers = ["id", "name", "addRowsAPIEnabled", "configuredBy", "isRefreshable", "isEffectiveIdentityRequired", "isEffectiveIdentityRolesRequired", "isOnPremGatewayRequired"]




    
for group_id, items in full_json_data.items():
        for item in items:
            if isinstance(item, dict):
                flattened_item = {"group_id": group_id}
                for key, value in item.items():
                    if isinstance(value, dict):
                        flattened_item.update({f"{key}.{k}": v for k, v in value.items()})
                    else:
                        flattened_item[key] = str(value) if isinstance(value, list) else value
                flat_data.append(flattened_item)
                

def call_power_bi_api_for_all_data(access_token, items_dict, api_name, item_type):
    """
    the function makes a generic API call for each item in each group.
    """
    
    all_data = []
    skipped_data = []

#Looping through each group ID and its corresponding list of item IDs
    for group_id, item_ids in items_dict.items():
        print(f"processing group id : {group_id}")
        # print(f"Type of item_ids: {type(item_ids)}")
        if not isinstance(item_ids, list):
            raise Exception(f"API response must be a list, but got type {type(item_ids)}")

        for item_id in item_ids:
            print(f"Calling API for item ID: {item_id}")
            if not isinstance(item_id, str):
                raise Exception(f"API response must be a list, but got type {type(item_id)}")

            for item_id in item_ids:
                if item_id is None:
                    print(f"Skipping item with no id for group id {group_id}")
                    continue
            print(f"Calling API for item ID: {item_id}")
            if not isinstance(item_id, str):
                raise Exception(f"API response must be a list, but got type {type(item_id)}")

            
            #Creating the API endpoint for the specific group, item type, and item ID
            endpoint=f"groups/{group_id}/{item_type}/{item_id}/{api_name}"
            # print(F"print api endpoint {endpoint}")


            #Call API and print the response
            try: 
                data = call_powerbi_api(access_token, endpoint)
                if isinstance(data, list):
                    all_data.extend(data)
                elif isinstance(data, dict):
                    all_data.append(data)
            except Exception as e:
                print(f"API call failed with Error: {e}")
                skipped_data.append(item_id)
                continue
        
    if skipped_data:
        print(f"Skipped data for {group_id} : {skipped_data}")

    return all_data
    



