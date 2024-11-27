[UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE] The Text datasource doesn't support the column `PTYID` of the type "DECIMAL(15,0)". SQLSTATE: 0A000
File <command-84585496061088>, line 17
     12     data_df.write.mode("overwrite").format("text") \
     13       .option("header", "true") \
     14       .save(file_path) 
     15     print(f"Saved data to {file_path}")
---> 17 fetch_vendor_data(view_names, base_path, source_table)
File /databricks/spark/python/pyspark/errors/exceptions/captured.py:261, in capture_sql_exception.<locals>.deco(*a, **kw)
    257 converted = convert_exception(e.java_exception)
    258 if not isinstance(converted, UnknownException):
    259     # Hide where the exception came from that shows a non-Pythonic
    260     # JVM exception message.
--> 261     raise converted from None
    262 else:
    263     raise
