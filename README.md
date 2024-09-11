AttributeError: 'list' object has no attribute 'status_code'
---------------------------------------------------------------------------
AttributeError                            Traceback (most recent call last)
File <command-3383306747622983>, line 26
     22                 raise Exception(f"Request failed with status {response.status_code},Response: {response.text}")
     24     return response_json, powerbi_object_Ids
---> 26 print(get_all_object_id_for_each_object_type(access_token, 'datasets', 'id')[1])

File <command-3383306747622983>, line 15, in get_all_object_id_for_each_object_type(access_token, object_type, key_id)
     13 endpoint = f"/groups/{group_id}/{object_type}"
     14 response = call_powerbi_api(access_token, endpoint)
---> 15 if response.status_code == 200:
     16     items = response.json().get('value', [])
     17     response_json.append(response.json().get('value', []))

AttributeError: 'list' object has no attribute 'status_code'
