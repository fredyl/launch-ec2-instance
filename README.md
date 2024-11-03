[ATTRIBUTE_NOT_SUPPORTED] Attribute `read` is not supported.
File <command-2638019235405315>, line 33
     31 result_df = batch_df.withColumn("data", fetch_data_udf(col("url"))).cache()
     32 batch_df = spark.read.json(f"{chk_path}")
---> 33 output_df = spark.read.json(result_df.select("data").read.map(lambda x: x.data))
File /databricks/spark/python/pyspark/sql/dataframe.py:3753, in DataFrame.__getattr__(self, name)
   3720 """Returns the :class:`Column` denoted by ``name``.
   3721 
   3722 .. versionadded:: 1.3.0
   (...)
   3750 +---+
   3751 """
   3752 if name not in self.columns:
-> 3753     raise PySparkAttributeError(
   3754         error_class="ATTRIBUTE_NOT_SUPPORTED", message_parameters={"attr_name": name}
   3755     )
   3756 jc = self._jdf.apply(name)
   3757 return Column(jc)
