IllegalArgumentException: java.net.URISyntaxException: Relative path in absolute URI: Error:%20400
File <command-2690687946717719>, line 63
     57             print(f"No data fetched")
     59     dbutils.fs.rm(d_path, True) #deleting the directory after processing the data
---> 63 get_billing_data()
File /databricks/spark/python/pyspark/errors/exceptions/captured.py:261, in capture_sql_exception.<locals>.deco(*a, **kw)
    257 converted = convert_exception(e.java_exception)
    258 if not isinstance(converted, UnknownException):
    259     # Hide where the exception came from that shows a non-Pythonic
    260     # JVM exception message.
--> 261     raise converted from None
    262 else:
    263     raise
