[CANNOT_DETERMINE_TYPE] Some of types cannot be determined after inferring.
File <command-310280906947066>, line 46
     44 #convert data to dataframe and upsert to the table
     45 if all_events:
---> 46     df = spark.createDataFrame(all_events_rows)
     47     # df = spark.createDataFrame(all_events)
     48     current_time = current_timestamp()
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
