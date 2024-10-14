[CANNOT_DETERMINE_TYPE] Some of types cannot be determined after inferring.
File <command-310280906947065>, line 46
     43 print("Pagination completed")
     45 #convert the data to dataframe, add timestamps columns and perform an upsert operation to the table using the upsert_data function
---> 46 df = spark.createDataFrame(all_events_metadata) 
     47 current_time = F.current_timestamp()
     48 df = df.withColumn("tg_inserted", current_time).withColumn("tg_updated", current_time)
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
