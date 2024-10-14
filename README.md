[DELTA_MERGE_UNRESOLVED_EXPRESSION] Cannot resolve year in UPDATE clause given columns existing_data.dcVehicleId, existing_data.devices, existing_data.groupId, existing_data.id, existing_data.isWaking, existing_data.lastCommunication, existing_data.lastWakeAttempt, existing_data.name, existing_data.status, existing_data.wakeable, existing_data.tg_inserted, existing_data.tg_updated. SQLSTATE: 42601
File <command-1127181961194070>, line 18
     16 df_sorted = df_partitioned.sortWithinPartitions('id',F.desc('lastConnected'))
     17 df = df_sorted.dropDuplicates(subset=['id'])
---> 18 upsert_data(df,table_name,current_time,primary_key='id',endpoint=None)
File /databricks/spark/python/pyspark/errors/exceptions/captured.py:261, in capture_sql_exception.<locals>.deco(*a, **kw)
    257 converted = convert_exception(e.java_exception)
    258 if not isinstance(converted, UnknownException):
    259     # Hide where the exception came from that shows a non-Pythonic
    260     # JVM exception message.
--> 261     raise converted from None
    262 else:
    263     raise
