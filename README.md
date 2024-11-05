dbutils.fs.mkdirs('/Volumes/{env}/bronze_vendor/holmanP/{data_type}')


def fetch_data_from_url(url, page, data_type, code_value):
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f"Bearer {token}"
    }
    response = requests.get(url, headers=headers)
    filePath = f'{chk_path}{data_type}_{code_value}_{page}.json'
    if response.status_code != 200:
        return f"Error: {response.status_code}"
    
    # with open(chk_path, 'w') as f:
    #     f.write(response.text)

    with open(filePath, 'w') as f:
        f.write(response.text)
    
    return filePath


https://github.com/user-attachments/assets/6bbc53c6-224c-4822-8c4d-d79f197e4343





PythonException: 
  An exception was thrown from the Python worker. Please see the stack trace below.
Traceback (most recent call last):
  File "/databricks/spark/python/pyspark/worker.py", line 1980, in main
    process()
  File "/databricks/spark/python/pyspark/worker.py", line 1972, in process
    serializer.dump_stream(out_iter, outfile)
  File "/databricks/spark/python/pyspark/serializers.py", line 307, in dump_stream
    self.serializer.dump_stream(self._batched(iterator), stream)
  File "/databricks/spark/python/pyspark/serializers.py", line 156, in dump_stream
    for obj in iterator:
  File "/databricks/spark/python/pyspark/serializers.py", line 300, in _batched
    chunk = list(itertools.islice(iterator, self.batchSize))
            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/databricks/spark/python/pyspark/worker.py", line 1863, in mapper
    return f(*[a[o] for o in args_offsets])
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/databricks/spark/python/pyspark/util.py", line 131, in wrapper
    return f(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^
TypeError: fetch_data_from_url() takes 4 positional arguments but 5 were given
File <command-2638019235405315>, line 47
     43 batch_df = batch_df.withColumn("part", floor(col("pageNumber") / 100)).repartition(num_batches, "part")
     44 result_df = batch_df.withColumn("data", fetch_data_udf(col("url"), col("pageNumber"), lit(data_type), lit(code_value), lit(d_path))).cache()
---> 47 file_path = [row.data for row in result_df.select("data").collect()]
     48 output_df = spark.read.json(file_path)
     49 json_data = output_df.select(data_key).toJSON().collect()
File /databricks/spark/python/pyspark/errors/exceptions/captured.py:261, in capture_sql_exception.<locals>.deco(*a, **kw)
    257 converted = convert_exception(e.java_exception)
    258 if not isinstance(converted, UnknownException):
    259     # Hide where the exception came from that shows a non-Pythonic
    260     # JVM exception message.
--> 261     raise converted from None
    262 else:
    263     raise
