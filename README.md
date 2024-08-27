The 'value' key was not found in the API response.

UnboundLocalError: local variable 'data_rdd' referenced before assignment
File <command-1374540070361438>, line 7, in create_dataframe(api_name, group_ids)
      6 try:
----> 7     data_rdd = spark.sparkContext.parallelize([json.dumps(full_json_data['value'])])
      8 except KeyError:
KeyError: 'value'

During handling of the above exception, another exception occurred:
UnboundLocalError                         Traceback (most recent call last)
File <command-1374540070361447>, line 2
      1 api_name = "datasets"
----> 2 dataset_df = create_dataframe("datasets",group_ids )
File <command-1374540070361438>, line 12, in create_dataframe(api_name, group_ids)
      9     print("The 'value' key was not found in the API response.")
     11     # data_rdd = spark.sparkContext.parallelize([json.dumps(get_json_data['value'])])
---> 12     data_df = spark.read.json(data_rdd)
     13     data_df.show()
     15 return data_df
