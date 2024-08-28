def groups_data(access_token):
    groups_data = get_groups_id(access_token)
    groups_data = [json.dumps(record) for record in groups_data.get("groups", [])]
    data_rdd = spark.sparkContext.parallelize(groups_data)
    data_df = spark.read.json(data_rdd)
    display(data_df)
    return data_df
