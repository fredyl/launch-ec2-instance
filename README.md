IndexError: list index out of range
---------------------------------------------------------------------------
IndexError                                Traceback (most recent call last)
File <command-3383306747622983>, line 26
     22                 raise Exception(f"Request failed with status {response.status_code},Response: {response.text}")
     24     return response_json, powerbi_object_Ids
---> 26 print(get_all_object_id_for_each_object_type(access_token, 'datasets', 'id')[1])

File <command-3383306747622983>, line 14, in get_all_object_id_for_each_object_type(access_token, object_type, key_id)
     12 for group_id in group_ids:
     13     endpoint = f"/groups/{group_id}/{object_type}"
---> 14     response = call_powerbi_api(access_token, endpoint)[1]
     15     if response.status_code == 200:
     16         items = response.json().get('value', [])

IndexError: list index out of range
