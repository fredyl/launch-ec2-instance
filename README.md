[PATH_NOT_FOUND] Path does not exist: dbfs:/Volumes/dev/bronze_vendor/holman/*.json. SQLSTATE: 42K03
File <command-2638019235405315>, line 34
     31 batch_df = batch_df.withColumn("part", floor(col("pageNumber") / 100)).repartition(num_batches, "part")
     32 result_df = batch_df.withColumn("data", fetch_data_udf(col("url"), col("pageNumber"))).cache()
---> 34 batch_df = spark.read.json(f"{chk_path}*.json")
     35 file_path = [row.data for row in result_df.select("data").collect()]
     36 print(len(file_path))
File /databricks/spark/python/pyspark/instrumentation_utils.py:47, in _wrap_function.<locals>.wrapper(*args, **kwargs)
     45 start = time.perf_counter()
     46 try:
---> 47     res = func(*args, **kwargs)
     48     logger.log_success(
     49         module_name, class_name, function_name, time.perf_counter() - start, signature
     50     )
     51     return res
File /databricks/spark/python/pyspark/sql/readwriter.py:467, in DataFrameReader.json(self, path, schema, primitivesAsString, prefersDecimal, allowComments, allowUnquotedFieldNames, allowSingleQuotes, allowNumericLeadingZero, allowBackslashEscapingAnyCharacter, mode, columnNameOfCorruptRecord, dateFormat, timestampFormat, multiLine, allowUnquotedControlChars, lineSep, samplingRatio, dropFieldIfAllNull, encoding, locale, pathGlobFilter, recursiveFileLookup, modifiedBefore, modifiedAfter, allowNonNumericNumbers)
    465 if type(path) == list:
    466     assert self._spark._sc._jvm is not None
--> 467     return self._df(self._jreader.json(self._spark._sc._jvm.PythonUtils.toSeq(path)))
    469 if not is_remote_only():
    470     from pyspark.core.rdd import RDD  # noqa: F401
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



Failed to request /ajax-api/2.0/fs/list?path=%2FVolumes%2Fdev%2Fbronze_vendor%2Fholman&page_size=1000: 404 Not Found The directory being accessed is not found.
