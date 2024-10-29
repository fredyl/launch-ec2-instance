PythonException: 
  An exception was thrown from the Python worker. Please see the stack trace below.
Traceback (most recent call last):
  File "/root/.ipykernel/991/command-1723439101937605-4150301398", line 16, in <lambda>
  File "/root/.ipykernel/991/command-1723439101937605-4150301398", line 6, in fetch_data_from_url
  File "/root/.ipykernel/991/command-4434650908684363-2091240785", line 22, in get_holman_api_response
Exception: ('Failed:', 404, '')
File <command-1723439101937788>, line 35
     31 # result_df.show()
     32 
     33 # Collect and process each page's data
     34 data_list = []
---> 35 for row in results_df.collect():
     36     if row.data:
     37         try:
File /databricks/spark/python/pyspark/errors/exceptions/captured.py:261, in capture_sql_exception.<locals>.deco(*a, **kw)
    257 converted = convert_exception(e.java_exception)
    258 if not isinstance(converted, UnknownException):
    259     # Hide where the exception came from that shows a non-Pythonic
    260     # JVM exception message.
--> 261     raise converted from None
    262 else:
    263     raise
