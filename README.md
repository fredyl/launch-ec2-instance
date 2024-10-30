[NOT_CALLABLE] Argument `func` should be a callable, got Column.
File <command-1723439101937786>, line 34
     32 batch_df = batch_df.withColumn("part", floor(col("pageNumber") / 100)).repartition(loops, "part")
     33 batch_df.show()
---> 34 result_df = batch_df.withColumn("data", fetch_data_udf(col("url"))).cache()
     35 result_df.show()
File /databricks/spark/python/pyspark/sql/udf.py:152, in _create_py_udf(f, returnType, useArrow)
    145     else:
    146         warnings.warn(
    147             "Arrow optimization for Python UDFs cannot be enabled for functions"
    148             " without arguments.",
    149             UserWarning,
    150         )
--> 152 return _create_udf(f, returnType, eval_type)
File /databricks/spark/python/pyspark/sql/udf.py:83, in _create_udf(f, returnType, evalType, name, deterministic)
     81 """Create a regular(non-Arrow-optimized) Python UDF."""
     82 # Set the name of the UserDefinedFunction object to be the name of function f
---> 83 udf_obj = UserDefinedFunction(
     84     f, returnType=returnType, name=name, evalType=evalType, deterministic=deterministic
     85 )
     86 return udf_obj._wrapped()
File /databricks/spark/python/pyspark/sql/udf.py:185, in UserDefinedFunction.__init__(self, func, returnType, name, evalType, deterministic)
    176 def __init__(
    177     self,
    178     func: Callable[..., Any],
   (...)
    182     deterministic: bool = True,
    183 ):
    184     if not callable(func):
--> 185         raise PySparkTypeError(
    186             error_class="NOT_CALLABLE",
    187             message_parameters={"arg_name": "func", "arg_type": type(func).__name__},
    188         )
    190     if not isinstance(returnType, (DataType, str)):
    191         raise PySparkTypeError(
    192             error_class="NOT_DATATYPE_OR_STR",
    193             message_parameters={
   (...)
    196             },
    197         )
