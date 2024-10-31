df = df.filter(~col(primary_key).isin(duplicate_id) | (col("some_other_column") != "some_value"))
[DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE] Cannot perform Merge as multiple source rows matched and attempted to modify the same
target row in the Delta table in possibly conflicting ways. By SQL semantics of Merge,
when multiple source rows match on the same target row, the result may be ambiguous
as it is unclear which source row should be used to update or delete the matching
target row. You can preprocess the source table to eliminate the possibility of
multiple matches. Please refer to
https://docs.microsoft.com/azure/databricks/delta/merge#merge-error SQLSTATE: 21506
File <command-310280906946883>, line 16
     14 # print(json.dumps(data_list, indent=4))
     15 if data_list:
---> 16     Holman_Upsert_data(data_type, data_key, data_list, primary_key)
     17 else:
     18     print(f"No data found for {data_type}_{data_key}")
File /databricks/spark/python/pyspark/errors/exceptions/captured.py:261, in capture_sql_exception.<locals>.deco(*a, **kw)
    257 converted = convert_exception(e.java_exception)
    258 if not isinstance(converted, UnknownException):
    259     # Hide where the exception came from that shows a non-Pythonic
    260     # JVM exception message.
--> 261     raise converted from None
    262 else:
    263     raise
