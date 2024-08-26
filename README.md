def create_dataframe(api_name, group_ids):
    combined_df = None
    for groups_d in group_ids:
        get_json_data = get_Item_ids_for_all_group_json(access_token, group_ids, api_name)

        if 'value' in get_json_data:
            data_rdd = spark.sparkContext.parallelize([json.dumps(get_json_data["value"])])
    data_df = spark.read.json(data_rdd)

    if combined_df is None:
        combined_df = data_df
    else:
        combined_df = combined_df.unionByName(data_df, allowMissingColumns=True)
    return combined_df
