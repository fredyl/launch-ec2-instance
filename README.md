elif object_type is not None:
   json_object = get_all_object_id_for_each_object_type(access_token, object_type, key_id)[1]
    json_string = json.dumps(json_object)
    json_rdd = spark.sparkContext.parallelize([json_string])
    spark_df = spark.read.option("multiLine", True).json(json_rdd)
     primary_key = spark_df[primary_key]
