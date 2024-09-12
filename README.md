def get_object_type_items(access_token, object_type, object_id, sub_api_endpoint):
    """
    retrieves items from object_type (e.g. datasets, reports, dataflows) based on the
    object_id's
    """
    groups_ids = get_all_groups_ids(access_token)
    group_id_and_ojbect_id_dict = get_item_ids_for_all_groups(access_token, groups_ids, object_type, object_id)
    all_data = []

    for group_ids, object_id in group_id_and_ojbect_id_dict.items():
        # print(group_ids, object_id)
        for group_id in group_ids:
            endpoint= f"groups/{group_id}/{object_type}/{object_id}/{sub_api_endpoint}"
