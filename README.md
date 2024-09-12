def get_object_type_items(access_token, object_type, object_id, sub_api_endpoint):
    """
    Retrieves items from object_type (e.g. datasets, reports, dataflows) based on the object_id.
    """
    groups_ids = get_all_groups_ids(access_token)
    group_id_and_object_id_dict = get_item_ids_for_all_groups(access_token, groups_ids, object_type, object_id)
    all_data = []
    
      for group_id, object_ids in group_id_and_object_id_dict.items():
        print(f"Processing group: {group_id}")
        for obj_id in object_ids:
            endpoint = f"groups/{group_id}/{object_type}/{obj_id}/{sub_api_endpoint}"
            try:
                # Call the API and append the response
                response, json_data = call_powerbi_api(access_token, endpoint)
                all_data.append(json_data)
            except HTTPError as e:
                # Handle 403 errors gracefully
                if e.response.status_code == 403:
                    print(f"Unauthorized access for group {group_id}. Skipping...")
                else:
                    raise  # Reraise the error for other types of HTTP errors

    return all_data
