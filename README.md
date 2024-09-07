def get_Item_ids_for_all_groups(access_token, group_ids, api_name, id_key =None, return_type ="json"):
    results = {}
    for group_id in group_ids:
        endpoint = f"groups/{group_id}/{api_name}"
        try:
            response_data = call_powerbi_api(access_token, endpoint)
            if isinstance(response_data, dict):
                items = response_data.get('value', [])
            elif isinstance(response_data, list):
                items = {'value': response_data}
            else:
                raise Exception(f"unexpected response format for group {group_id}")
        
            if return_type == "ids":
                if id_key is not None:
                    raise ValueError(f"id_key must be specified for return_type 'ids'")
                results[group_id] = [item.get(id_key) for item in response_data if item.get(id_key) is not None] 
                    # return [group_id for group_id, items in results.items() if items is not None]
            elif return_type == "json":
                results[group_id] = items
                        # results[group_id] = [ item for item in response_data.get("value") if item is not None]
                return results
            else:
                raise Exception(f"Invalid return type . Use 'ids' or 'json'")
        except Exception as e:
            raise Exception(f"unexpected response format for group {group_id}: {e}")

    return results


    'd2743458-256a-493c-80fe-43473ec81cd3': [],
 '76588501-fc39414c4eaa': [],
 '6ccf1a1b-aabf906e52d5': [],
 'e22480f3-5017e044d744': [],
 '485c3f61-cafb637cdb8c': [],
 'f014186c-96ad307adecf': []}
