def get_object_type_items(access_token, object_type, object_id, sub_api_endpoint):
    """
    Retrieves items from object_type (e.g. datasets, reports, dataflows) based on the object_id.
    """
    groups_ids = get_all_groups_ids(access_token)
    group_id_and_object_id_dict = get_item_ids_for_all_groups(access_token, groups_ids, object_type, object_id)
    all_data = []

    for group_id, object_ids in group_id_and_object_id_dict.items():
        for obj_id in object_ids:
            endpoint = f"groups/{group_id}/{object_type}/{obj_id}/{sub_api_endpoint}"
            print(f"Processing: {endpoint}")

            try:
                # Call Power BI API and retrieve the response
                response, status_code = call_powerbi_api(access_token, endpoint)
                
                # Skip if the dataset is not model-based (handle 415 error)
                if status_code == 415:
                    print(f"Skipping dataset {obj_id} in group {group_id} - API not supported for this dataset type.")
                    continue

                # If the response has a 403 status code, skip and continue
                if status_code == 403:
                    print(f"Unauthorized access for group {group_id}. Skipping...")
                    continue
                
                # If the response has a 200 status code, append the data
                if status_code == 200:
                    all_data.append(response)
                    print(f"Successfully retrieved data for {group_id}")

            except Exception as e:
                print(f"Error for group {group_id}: {e}")
                continue  # Continue to next group if there's an error

    return all_data