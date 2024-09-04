"""
Processing various sub power bi apis and creating or updating the data to delta table, from
url = https://api.powerbi.com/v1.0/myorg/groups/{group_id}/{api_name}/{name}
"""
api_configs = [
    {
        "primary_keys" : ["datasourceId"],
        "table_name" : "dev.bronze.pbi_dataflows_datasource",
        "id_field" : "objectId",
        "sub_api" : "datasources",
        "api_name": "dataflows"
    },
    {
        "primary_keys" : ["datasourceId"],
        "table_name" : "dev.bronze.pbi_datasets_datasource",
        "id_field" : "id",
        "sub_api" : "datasources",
        "api_name": "datasets"
    },
    {
        "primary_keys" : ["name"],
        "table_name" : "dev.bronze.pbi_reports_page",
        "id_field" : "id",
        "sub_api" : "pages",
        "api_name": "reports"
    },    
]
for config in api_configs:
    api_name = config["api_name"]
    table_name = config["table_name"]
    primary_keys = config["primary_keys"]
    sub_api = config.get("sub_api")
    id_field = config.get("id_field")



    try:
        access_token = get_access_token(client_id, client_secret, tenant_id, scope)
        group_ids = get_all_groups_ids(access_token)
        data_ids_dict = get_Item_ids_for_all_groups(access_token, group_ids, api_name, id_field, retrurn_full_json =False)
        all_data = call_power_bi_api_for_all_data(access_token, data_ids_dict, sub_api, api_name, id_field)
        headers, rows = convert_to_table_structure(all_data)
        create_or_update_table_1(headers,rows,table_name,primary_keys)
    except Exception as e: 
        print(f"Failed while processing API '{api_name}/{sub_api}' for table '{table_name}': {e}")
