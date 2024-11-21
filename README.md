[UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE] The Text datasource doesn't support the column `NPFSEQ` of the type "DECIMAL(5,0)". SQLSTATE: 0A000
File <command-265156676446858>, line 1
----> 1 process_saved_views(control_table)
File /databricks/spark/python/pyspark/errors/exceptions/captured.py:261, in capture_sql_exception.<locals>.deco(*a, **kw)
    257 converted = convert_exception(e.java_exception)
    258 if not isinstance(converted, UnknownException):
    259     # Hide where the exception came from that shows a non-Pythonic
    260     # JVM exception message.
--> 261     raise converted from None
    262 else:
    263     raise
