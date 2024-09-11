Running get_object_type_items
Running get_all_object_id_for_each_object_type
369
369
369
369
369
369
369
Exception: Request failed with status 415,Response: {"error":{"code":"InvalidRequest","message":"Invalid dataset. This API can only be called on a Model-based dataset"}}
File <command-3383306747622986>, line 56
     48                     print(items_llist)
     49                 # else:
     50         #             items = response.json().get('value', [])
     51         #             for item in items:
     52         #                 item[key_id] = object_id
     53         #             items_llist.extend(items)
     54         # return items_llist
---> 56 print(get_object_type_items(access_token, 'datasets', 'id', 'refreshes'))
File <command-3383306747622981>, line 16, in call_powerbi_api(access_token, endpoint, params)
     14     time.sleep(retry_after)
     15 else:
---> 16     raise Exception(f"Request failed with status {response.status_code},Response: {response.text}")



for group_id in group_ids:
        for object_id in object_ids:
            print(len(object_ids))
            endpoint = f"/groups/{group_id}/{object_type}/{object_id}/{sub_api_endpoint}"
            status_code, json_data  = call_powerbi_api(access_token, endpoint)
            if status_code == 200:
                if sub_api_endpoint == "refreshSchedule":#sub_api_endpoint is refreshSchedule the process since the response is a json object
                    items = json_data.get("model") == 'datasets'
                    for item in items:
                #     items = response.json()
                        items[key_id] = object_id
                    items_llist.append(items)
                    print(items_llist)
                # else:
        #             items = response.json().get('value', [])
        #             for item in items:
        #                 item[key_id] = object_id
        #             items_llist.extend(items)
        # return items_llist
    
print(get_object_type_items(access_token, 'datasets', 'id', 'refreshes'))
