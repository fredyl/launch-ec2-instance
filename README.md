[UNABLE_TO_INFER_SCHEMA] Unable to infer schema for JSON. It must be specified manually. SQLSTATE: 42KD9
File <command-2638019235405315>, line 42
     39 result_df = batch_df.withColumn("data", fetch_data_udf(col("url"), col("pageNumber"), lit(data_type), lit(code_value))).cache()
     40 # display(result_df)
---> 42 batch_df = spark.read.json(f"{chk_path}")
     43 file_path = [row.data for row in result_df.select("data").collect()]
     44 # print(len(file_path))
File /databricks/spark/python/pyspark/errors/exceptions/captured.py:261, in capture_sql_exception.<locals>.deco(*a, **kw)
    257 converted = convert_exception(e.java_exception)
    258 if not isinstance(converted, UnknownException):
    259     # Hide where the exception came from that shows a non-Pythonic
    260     # JVM exception message.
--> 261     raise converted from None
    262 else:
    263     raise
