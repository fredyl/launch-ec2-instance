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
    


Exception: API response must be a list, but got type <class 'NoneType'>
---------------------------------------------------------------------------
Exception                                 Traceback (most recent call last)
File <command-1374540070361322>, line 8
      1 # test_dict = {"485c3f61-5ecd-4bc1-a96c-cafb637cdb8c": [
      2 #     "ba83945a-2740-4ca5-880a-cb8469115188",
      3 #     "3db4ca48-4f34-447a-b2fc-e39d1cff532f",
   (...)
      6 #     "d03721e5-bc60-4eb3-968b-6bb6d7e791be"
      7 #   ],}
----> 8 all_data = call_power_bi_api_for_all_data(access_token, dataset_ids_dict, "datasources", "datasets")
      9 print(all_data)

File <command-1374540070361312>, line 19, in call_power_bi_api_for_all_data(access_token, items_dict, api_name, item_type)
     17 print(f"Calling API for item ID: {item_id}")
     18 if not isinstance(item_id, str):
---> 19     raise Exception(f"API response must be a list, but got type {type(item_id)}")
     21 for item_id in item_ids:
     22     if item_id is None:

Exception: API response must be a list, but got type <class 'NoneType'>


