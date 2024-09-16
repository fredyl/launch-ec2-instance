[DELTA_MERGE_UNRESOLVED_EXPRESSION] Cannot resolve n.LastModified in UPDATE clause given columns t.endTime, t.id, t.refreshAttempts, t.refreshType, t.requestId, t.serviceExceptionJson, t.startTime, t.status, t.LastModified, t.insertDatetime, n.endTime, n.id, n.refreshAttempts, n.refreshType, n.requestId, n.serviceExceptionJson, n.startTime, n.status. SQLSTATE: 42601
File <command-2798623019032397>, line 3
      1 #getting data and creating or updataing the table for datasets refreshes
      2 print("Running datasets refreshes Power BI API Call")
----> 3 create_dataframe_and_update_or_merge_table(access_token=access_token, table_name="bronze.pbi_datasets_refreshes", primary_key=["requestId"],api_flag=None, object_type="datasets", key_id="id", sub_api_endpoint="refreshes")
      6 # #getting data and creating or updataing the table for dataflows transactions
      7 print("Running dataflows transactions Power BI API Call")
File /databricks/spark/python/pyspark/errors/exceptions/captured.py:261, in capture_sql_exception.<locals>.deco(*a, **kw)
    257 converted = convert_exception(e.java_exception)
    258 if not isinstance(converted, UnknownException):
    259     # Hide where the exception came from that shows a non-Pythonic
    260     # JVM exception message.
--> 261     raise converted from None
    262 else:
    263     raise
