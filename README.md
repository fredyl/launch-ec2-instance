'''
define a set of endpoint for corresponding data keys, creating a merge key used to match rows between the source df and delta table
'''

data_type="orders" 
data_key="orderHistory"
primary_key="clientVehicleNumber"
merge_keys= ["clientVehicleNumber", "upfitPoIssueDate","tg_updated"]  
table_name = f"bronze.holman_{data_type}"


upd_endpoints = update_endpoints(data_type)

#
if upd_endpoints and upd_endpoints.get("run_all"):
    data_list = get_holman_data(token, data_type=data_type, data_key=data_key)

Holman_Upsert_data(data_type,data_list, primary_key, merge_keys)
print(f"Data upserted for data type {data_type} with code value {code_value}")


[DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE] Cannot perform Merge as multiple source rows matched and attempted to modify the same
