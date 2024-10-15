[CANNOT_DETERMINE_TYPE] Some of types cannot be determined after inferring.
File <command-310280906946865>, line 6
      4 code = 2
      5 page = 1
----> 6 Holman_Upsert_data(data_type="billing", code_key="billingTypeCode", data_key="billing",table_name=None, primary_key="vehicleNumber")
File <command-3234459011987704>, line 9, in Holman_Upsert_data(data_type, code_key, data_key, table_name, primary_key)
      7 print(json.dumps(data_list, indent =2))
      8 updated_data = replace_null_values(data_list)
----> 9 df = spark.createDataFrame(data_list)
     10 display(df)
File /databricks/spark/python/pyspark/instrumentation_utils.py:47, in _wrap_function.<locals>.wrapper(*args, **kwargs)
     45 start = time.perf_counter()
     46 try:
---> 47     res = func(*args, **kwargs)
     48     logger.log_success(
     49         module_name, class_name, function_name, time.perf_counter() - start, signature
     50     )
     51     return res
File /databricks/spark/python/pyspark/sql/session.py:1605, in SparkSession.createDataFrame(self, data, schema, samplingRatio, verifySchema)
   1600 if has_pandas and isinstance(data, pd.DataFrame):
   1601     # Create a DataFrame from pandas DataFrame.
   1602     return super(SparkSession, self).createDataFrame(  # type: ignore[call-overload]
   1603         data, schema, samplingRatio, verifySchema
   1604     )
-> 1605 return self._create_dataframe(
   1606     data, schema, samplingRatio, verifySchema  # type: ignore[arg-type]
   1607 )
File /databricks/spark/python/pyspark/sql/session.py:1662, in SparkSession._create_dataframe(self, data, schema, samplingRatio, verifySchema)
   1660     rdd, struct = self._createFromRDD(data.map(prepare), schema, samplingRatio)
   1661 else:
-> 1662     rdd, struct = self._createFromLocal(map(prepare, data), schema)
   1663 jrdd = self._jvm.SerDeUtil.toJavaArray(rdd._to_java_object_rdd())
   1664 jdf = self._jsparkSession.applySchemaToPythonRDD(jrdd.rdd(), struct.json())
File /databricks/spark/python/pyspark/sql/session.py:1229, in SparkSession._createFromLocal(self, data, schema)
   1221 def _createFromLocal(
   1222     self, data: Iterable[Any], schema: Optional[Union[DataType, List[str]]]
   1223 ) -> Tuple["RDD[Tuple]", StructType]:
   1224     """
   1225     Create an RDD for DataFrame from a list or pandas.DataFrame, returns the RDD and schema.
   1226     This would be broken with table acl enabled as user process does not have permission to
   1227     write temp files.
   1228     """
-> 1229     internal_data, struct = self._wrap_data_schema(data, schema)
   1230     return self._sc.parallelize(internal_data), struct
File /databricks/spark/python/pyspark/sql/session.py:1196, in SparkSession._wrap_data_schema(self, data, schema)
   1193     data = list(data)
   1195 if schema is None or isinstance(schema, (list, tuple)):
-> 1196     struct = self._inferSchemaFromList(data, names=schema)
   1197     converter = _create_converter(struct)
   1198     tupled_data: Iterable[Tuple] = map(converter, data)
File /databricks/spark/python/pyspark/sql/session.py:1076, in SparkSession._inferSchemaFromList(self, data, names)
   1061 schema = reduce(
   1062     _merge_type,
   1063     (
   (...)
   1073     ),
   1074 )
   1075 if _has_nulltype(schema):
-> 1076     raise PySparkValueError(
   1077         error_class="CANNOT_DETERMINE_TYPE",
   1078         message_parameters={},
   1079     )
   1080 return schema
