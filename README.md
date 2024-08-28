api_name = "datasets"
full_json_data = get_Item_ids_for_all_group_json(access_token, group_ids, api_name)
data_df = create_dataframe("datasets", full_json_data)
table_name = "dev.bronze.pbi_datasets"
api_name = "datasets"
primary_keys =["id"]
create_or_update_table_2( data_df, table_name, primary_keys)

api_name = "dataflows"
full_json_data = get_Item_ids_for_all_group_json(access_token, group_ids, api_name)
data_df = create_dataframe("dataflows", full_json_data)
table_name = "dev.bronze.pbi_dataflows"
api_name = "dataflows"
primary_keys =["objectId"]
create_or_update_table_2( data_df, table_name, primary_keys)

api_name = "reports"
full_json_data = get_Item_ids_for_all_group_json(access_token, group_ids, api_name)
data_df = create_dataframe("reports", full_json_data)
table_name = "dev.bronze.pbi_reports"
api_name = "reports"
primary_keys =["id"]
create_or_update_table_2( data_df, table_name, primary_keys)
