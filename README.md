[DELTA_MERGE_UNRESOLVED_EXPRESSION] Cannot resolve coachEmployeeNum in INSERT clause given columns new_data.id, new_data.behaviors, new_data.coachingSessionNotes, new_data.eventNotes, new_data.notes, new_data.reviewNotes, new_data.tg_inserted, new_data.tg_updated. SQLSTATE: 42601
File <command-310280906947065>, line 55
     53     current_time = current_timestamp()
     54     df = df.withColumn("tg_inserted", current_time).withColumn("tg_updated", current_time)
---> 55     upsert_data(df,table_name,current_time,complex_columns,'id',endpoint)
     56 else:
     57     print("No data recieved from API")
File /databricks/spark/python/pyspark/errors/exceptions/captured.py:261, in capture_sql_exception.<locals>.deco(*a, **kw)
    257 converted = convert_exception(e.java_exception)
    258 if not isinstance(converted, UnknownException):
    259     # Hide where the exception came from that shows a non-Pythonic
    260     # JVM exception message.
--> 261     raise converted from None
    262 else:
    263     raise
