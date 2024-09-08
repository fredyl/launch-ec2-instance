UnboundLocalError: cannot access local variable 'endpoint' where it is not associated with a value
File <command-1936225453607471>, line 24, in call_power_bi_api_for_all_data(access_token, data_ids_dict, item_type, api_name, id_field)
     23 try: 
---> 24     data = call_powerbi_api(access_token, endpoint)
     25     print(json.dumps(data, indent=4))
File <command-1936225453607471>, line 34, in call_power_bi_api_for_all_data(access_token, data_ids_dict, item_type, api_name, id_field)
     26 #             if isinstance(data, list):
     27 #                 for item in data:</span>
     28 #                     item[id_field] = item_id
   (...)
     31 #                 data[id_field] = item_id
     32 #                 all_data.append(data)
     33         except Exception as e:
---> 34             print(f"Failed to retrieve data for {endpoint}: {e}")
     35             # skipped_data.append(item_id)
     36             continue
