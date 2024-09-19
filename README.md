 MERGE INTO bronze.pbi_datasets_refreshschedule AS t
        USING new_data AS n
        ON t.days = n.days
        WHEN MATCHED THEN 
        UPDATE SET t.``@odata.context`` = n.``@odata.context``, t.days = n.days, t.enabled = n.enabled, t.id = n.id, t.localTimeZoneId = n.localTimeZoneId, t.notifyOption = n.notifyOption, t.times = n.times, t.LastModified = n.LastModified
        WHEN NOT MATCHED THEN 
        INSERT (``@odata.context``, days, enabled, id, localTimeZoneId, notifyOption, times, LastModified, InsertTime)
        VALUES (n.``@odata.context``, n.days, n.enabled, n.id, n.localTimeZoneId, n.notifyOption, n.times, n.LastModified, n.InsertTime)




        [PARSE_SYNTAX_ERROR] Syntax error at or near '@'. SQLSTATE: 42601
File <command-3684298126601210>, line 2
      1 print("Running datasets refreshschedule Power BI API Call")
----> 2 create_dataframe_and_update_or_merge_table(access_token=access_token, table_name="bronze.pbi_datasets_refreshschedule", primary_key=["days"],api_flag=None, object_type="datasets", key_id="id", sub_api_endpoint="refreshSchedule")
File <command-3301156189242308>, line 68, in create_dataframe_and_update_or_merge_table(access_token, table_name, primary_key, api_flag, object_type, key_id, sub_api_endpoint)
     58     # merge_query = f"""
     59     # MERGE INTO {table_name} AS t
     60     # USING new_data AS n
   (...)
     65     # INSERT *
     66     # """
     67     print(merge_query)
---> 68     spark.sql(merge_query)
     69 else: 
     70     print(f"Table {table_name} does not exist. Creating a new table.")
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
