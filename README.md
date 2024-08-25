dataset_ids_dict = get_Item_ids_for_all_groups(access_token, group_ids, "dataflows", "id")
    
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null
  ],
  "68f7d43a-c5b1-45bf-a559-d35fa3361a62": [
    null
call_power_bi_api_for_all_data(access_token, dataset_ids_dict, "transactions", "dataflows")

processing group id : 97b99d50-bc67-4e69-84d3-1c78e0c7db21
Type of item_ids: <class 'list'>
Calling API for item ID: None
Exception: API response must be a list, but got type <class 'NoneType'>
