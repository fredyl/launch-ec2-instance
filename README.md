table_name = "dev.bronze.pbi_reports_page"
primary_key = ["name"]
data_ids_dict = get_Item_ids_for_all_groups(access_token, group_ids, "reports", "id")
all_data = call_power_bi_api_for_all_data(access_token, data_ids_dict, "pages", "reports")
headers, rows = convert_to_table_structure(all_data)
create_or_update_table_1(headers,rows,table_name,primary_key)

table_name = "dev.bronze.pbi_dataflows_datasource"
primary_key = ["datasourceId"]
data_ids_dict = get_Item_ids_for_all_groups(access_token, group_ids, "dataflows", "objectId")
all_data = call_power_bi_api_for_all_data(access_token, data_ids_dict, "datasources", "dataflows")
headers, rows = convert_to_table_structure(all_data)
create_or_update_table_1(headers,rows,table_name,primary_key)

table_name = "dev.bronze.pbi_dataset_datasource"
primary_key = ["datasourceId"]
data_ids_dict = get_Item_ids_for_all_groups(access_token, group_ids, "datasets", "id")
all_data = call_power_bi_api_for_all_data(access_token, data_ids_dict, "datasources", "datasets")
headers, rows = convert_to_table_structure(all_data)
create_or_update_table_1(headers,rows,table_name,primary_key)
