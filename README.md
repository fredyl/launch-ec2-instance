eb3-968b-6bb6d7e791be', 'name': '_DataHub Import Model _Dev (UAT) - vs Plan', 'webUrl': 'https://app.powerbi.com/groups/485c3f61-5ecd-4bc1-a96c-cafb637cdb8c/datasets/d03721e5-bc60-4eb3-968b-6bb6d7e791be', 'addRowsAPIEnabled': False, 'configuredBy': 'jconnell01@Trugreenmail.Com', 'isRefreshable': True, 'isEffectiveIdentityRequired': False, 'isEffectiveIdentityRolesRequired': False, 'isOnPremGatewayRequired': False, 'targetStorageMode': 'Abf', 'createdDate': '2024-07-11T04:26:00.33Z', 'createReportEmbedURL': 'https://app.powerbi.com/reportEmbed?config=eyJjbHVzdGVyVXJsIjoiaHR0cHM6Ly9XQUJJLVVTLU5PUlRILUNFTlRSQUwtRy1QUklNQVJZLXJlZGlyZWN0LmFuYWx5c2lzLndpbmRvd3MubmV0IiwiZW1iZWRGZWF0dXJlcyI6eyJ1c2FnZU1ldHJpY3NWTmV4dCI6dHJ1ZX19', 'qnaEmbedURL': 'https://app.powerbi.com/qnaEmbed?config=eyJjbHVzdGVyVXJsIjoiaHR0cHM6Ly9XQUJJLVVTLU5PUlRILUNFTlRSQUwtRy1QUklNQVJZLXJlZGlyZWN0LmFuYWx5c2lzLndpbmRvd3MubmV0IiwiZW1iZWRGZWF0dXJlcyI6eyJ1c2FnZU1ldHJpY3NWTmV4dCI6dHJ1ZX19', 'upstreamDatasets': '[]', 'users': '[]', 'queryScaleOutSettings.autoSyncReadOnlyReplicas': True, 'queryScaleOutSettings.maxReadOnlyReplicas': 0}, {'group_id': 'f014186c-db9b-4c49-969b-96ad307adecf', 'id': 'dadb599e-c331-4209-8f4d-51257f94d792', 'name': 'AutoMeasure (Test Tier)', 'webUrl': 'https://app.powerbi.com/groups/f014186c-db9b-4c49-969b-96ad307adecf/datasets/dadb599e-c331-4209-8f4d-51257f94d792', 'addRowsAPIEnabled': False, 'configuredBy': 'jgary@Trugreenmail.Com', 'isRefreshable': True, 'isEffectiveIdentityRequired': False, 'isEffectiveIdentityRolesRequired': False, 'isOnPremGatewayRequired': True, 'targetStorageMode': 'Abf', 'createdDate': '2024-05-16T19:53:09.96Z', 'createReportEmbedURL': 'https://app.powerbi.com/reportEmbed?config=eyJjbHVzdGVyVXJsIjoiaHR0cHM6Ly9XQUJJLVVTLU5PUlRILUNFTlRSQUwtRy1QUklNQVJZLXJlZGlyZWN0LmFuYWx5c2lzLndpbmRvd3MubmV0IiwiZW1iZWRGZWF0dXJlcyI6eyJ1c2FnZU1ldHJpY3NWTmV4dCI6dHJ1ZX19', 'qnaEmbedURL': 'https://app.powerbi.com/qnaEmbed?config=eyJjbHVzdGVyVXJsIjoiaHR0cHM6Ly9XQUJJLVVTLU5PUlRILUNFTlRSQUwtRy1QUklNQVJZLXJlZGlyZWN0LmFuYWx5c2lzLndpbmRvd3MubmV0IiwiZW1iZWRGZWF0dXJlcyI6eyJ1c2FnZU1ldHJpY3NWTmV4dCI6dHJ1ZX19', 'upstreamDatasets': '[]', 'users': '[]', 'queryScaleOutSettings.autoSyncReadOnlyReplicas': True, 'queryScaleOutSettings.maxReadOnlyReplicas': 0}]


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
    



