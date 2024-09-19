[DELTA_MERGE_UNRESOLVED_EXPRESSION] Cannot resolve t.group_id in search condition given columns t.endTime, t.id, t.refreshAttempts, t.refreshType, t.requestId, t.serviceExceptionJson, t.startTime, t.status, t.LastModified, t.InsertTime, n.endTime, n.group_id, n.id, n.refreshAttempts, n.refreshType, n.requestId, n.serviceExceptionJson, n.startTime, n.status, n.LastModified, n.InsertTime. SQLSTATE: 42601
File <command-3684298126602363>, line 2
      1 print("Running datasets refreshes Power BI API Call")
----> 2 create_dataframe_and_update_or_merge_table(access_token=access_token, table_name="bronze.pbi_datasets_refreshes", primary_key=["requestId", "group_id"],api_flag=None, object_type="datasets", key_id="id", sub_api_endpoint="refreshes")
File <command-3301156189241440>, line 52, in create_dataframe_and_update_or_merge_table(access_token, table_name, primary_key, api_flag, object_type, key_id, sub_api_endpoint)
     42     merge_query = f"""
     43     MERGE INTO {table_name} AS t
     44     USING new_data AS n
   (...)
     49     INSERT *
     50     """
     51     # print(merge_query)
---> 52     spark.sql(merge_query)
     53 else: 
     54     print(f"Table {table_name} does not exist. Creating a new table.")
File /databricks/spark/python/pyspark/instrumentation_utils.py:47, in _wrap_function.<locals>.wrapper(*args, **kwargs)
     45 start = time.perf_counter()
     46 try:
---> 47     res = func(*args, **kwargs)
     48     logger.log_success(
     49         module_name, class_name, function_name, time.perf_counter() - start, signature
     50     )
     51     return res
File /databricks/spark/python/pyspark/sql/session.py:1825, in SparkSession.sql(self, sqlQuery, args, **kwargs)
   1820     else:
   1821         raise PySparkTypeError(
   1822             error_class="INVALID_TYPE",
   1823             message_parameters={"arg_name": "args", "arg_type": type(args).__name__},
   1824         )
-> 1825     return DataFrame(self._jsparkSession.sql(sqlQuery, litArgs), self)
   1826 finally:
   1827     if len(kwargs) > 0:
File /databricks/spark/python/lib/py4j-0.10.9.7-src.zip/py4j/java_gateway.py:1355, in JavaMember.__call__(self, *args)
   1349 command = proto.CALL_COMMAND_NAME +\
   1350     self.command_header +\
   1351     args_command +\
   1352     proto.END_COMMAND_PART
   1354 answer = self.gateway_client.send_command(command)
-> 1355 return_value = get_return_value(
   1356     answer, self.gateway_client, self.target_id, self.name)
   1358 for temp_arg in temp_args:
   1359     if hasattr(temp_arg, "_detach"):
File /databricks/spark/python/pyspark/errors/exceptions/captured.py:261, in capture_sql_exception.<locals>.deco(*a, **kw)
    257 converted = convert_exception(e.java_exception)
    258 if not isinstance(converted, UnknownException):
    259     # Hide where the exception came from that shows a non-Pythonic
    260     # JVM exception message.
--> 261     raise converted from None
    262 else:
    263     raise
