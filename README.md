Exception: Request failed with status 400,Response: {"error":{"code":"BadRequest","message":"Bad Request","details":[{"message":"The value '3' is not valid for Nullable`1.","target":"group"}]}}
File <command-3301156189241970>, line 46
     42                 print(f"unexpected error {e} for group {group_id}")
     44     return all_data
---> 46 get_object_type_items(access_token, "datasets", "id", "refreshSchedule")
File <command-3301156189241966>, line 19, in call_powerbi_api(access_token, endpoint, params)
     16     time.sleep(retry_after)
     18 else:
---> 19     raise Exception(f"Request failed with status {response.status_code},Response: {response.text}")
