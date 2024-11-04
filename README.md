PythonException: Traceback (most recent call last):
org.apache.spark.SparkException: Job aborted due to stage failure: Task 0 in stage 244.0 failed 4 times, most recent failure: Lost task 0.3 in stage 244.0 (TID 3583) (10.139.64.12 executor driver): org.apache.spark.api.python.PythonException: Traceback (most recent call last):
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
TypeError: fetch_data_from_url() missing 1 required positional argument: 'page'

	at org.apache.spark.api.python.BasePythonRunner$ReaderIterator.handlePythonException(PythonRunner.scala:551)
	at org.apache.spark.sql.execution.python.BasePythonUDFRunner$$anon$2.read(PythonUDFRunner.scala:115)
	at org.apache.spark.sql.execution.python.BasePythonUDFRunner$$anon$2.read(PythonUDFRunner.scala:98)
	at org.apache.spark.api.python.BasePythonRunner$ReaderIterator.hasNext(PythonRunner.scala:507)
	at org.apache.spark.InterruptibleIterator.hasNext(InterruptibleIterator.scala:37)
	at scala.collection.Iterator$$anon$11.hasNext(Iterator.scala:491)
	at scala.collection.Iterator$$anon$10.hasNext(Iterator.scala:460)
	at scala.collection.Iterator$$anon$10.hasNext(Iterator.scala:460)
	at org.apache.spark.sql.catalyst.expressions.GeneratedClass$GeneratedIteratorForCodegenStage2.processNext(Unknown Source)
	at org.apache.spark.sql.execution.BufferedRowIterator.hasNext(BufferedRowIterator.java:43)
	at org.apache.spark.sql.execution.WholeStageCodegenEvaluatorFactory$WholeStageCodegenPartitionEvaluator$$anon$1.hasNext(WholeStageCodegenEvaluatorFactory.scala:50)
	at org.apache.spark.sql.execution.columnar.DefaultCachedBatchSerializer$$anon$1.hasNext(InMemoryRelation.scala:124)
	at org.apache.spark.sql.execution.columnar.CachedRDDBuilder$$anon$2.hasNext(InMemoryRelation.scala:297)
	at org.apache.spark.storage.memory.MemoryStore.putIterator(MemoryStore.scala:229)
	at org.apache.spark.storage.memory.MemoryStore.putIteratorAsValues(MemoryStore.scala:314)
	at org.apache.spark.storage.BlockManager.$anonfun$doPutIterator$1(BlockManager.scala:1704)
	at org.apache.spark.storage.BlockManager.org$apache$spark$storage$BlockManager$$doPut(BlockManager.scala:1630)
	at org.apache.spark.storage.BlockManager.doPutIterator(BlockManager.scala:1695)
	at org.apache.spark.storage.BlockManager.getOrElseUpdate(BlockManager.scala:1489)
	at org.apache.spark.storage.BlockManager.getOrElseUpdateRDDBlock(BlockManager.scala:1443)
	at org.apache.spark.rdd.RDD.getOrCompute(RDD.scala:425)
	at org.apache.spark.rdd.RDD.iterator(RDD.scala:375)
	at org.apache.spark.scheduler.ResultTask.$anonfun$runTask$3(ResultTask.scala:82)
	at com.databricks.spark.util.ExecutorFrameProfiler$.record(ExecutorFrameProfiler.scala:110)
	at org.apache.spark.scheduler.ResultTask.$anonfun$runTask$1(ResultTask.scala:82)
	at com.databricks.spark.util.ExecutorFrameProfiler$.record(ExecutorFrameProfiler.scala:110)
	at org.apache.spark.scheduler.ResultTask.runTask(ResultTask.scala:62)
	at org.apache.spark.TaskContext.runTaskWithListeners(TaskContext.scala:211)
	at org.apache.spark.scheduler.Task.doRunTask(Task.scala:199)
	at org.apache.spark.scheduler.Task.$anonfun$run$5(Task.scala:161)
	at com.databricks.unity.UCSEphemeralState$Handle.runWith(UCSEphemeralState.scala:51)
	at com.databricks.unity.HandleImpl.runWith(UCSHandle.scala:104)
	at com.databricks.unity.HandleImpl.$anonfun$runWithAndClose$1(UCSHandle.scala:109)
	at scala.util.Using$.resource(Using.scala:269)
	at com.databricks.unity.HandleImpl.runWithAndClose(UCSHandle.scala:108)
	at org.apache.spark.scheduler.Task.$anonfun$run$1(Task.scala:155)
	at com.databricks.spark.util.ExecutorFrameProfiler$.record(ExecutorFrameProfiler.scala:110)
	at org.apache.spark.scheduler.Task.run(Task.scala:102)
	at org.apache.spark.executor.Executor$TaskRunner.$anonfun$run$10(Executor.scala:1036)
	at org.apache.spark.util.SparkErrorUtils.tryWithSafeFinally(SparkErrorUtils.scala:64)
	at org.apache.spark.util.SparkErrorUtils.tryWithSafeFinally$(SparkErrorUtils.scala:61)
	at org.apache.spark.util.Utils$.tryWithSafeFinally(Utils.scala:110)
	at org.apache.spark.executor.Executor$TaskRunner.$anonfun$run$3(Executor.scala:1039)
	at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)
	at com.databricks.spark.util.ExecutorFrameProfiler$.record(ExecutorFrameProfiler.scala:110)
	at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:926)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
	at java.lang.Thread.run(Thread.java:750)

Driver stacktrace:
	at org.apache.spark.scheduler.DAGScheduler.$anonfun$failJobAndIndependentStages$1(DAGScheduler.scala:3998)
	at scala.Option.getOrElse(Option.scala:189)
	at org.apache.spark.scheduler.DAGScheduler.failJobAndIndependentStages(DAGScheduler.scala:3996)
	at org.apache.spark.scheduler.DAGScheduler.$anonfun$abortStage$2(DAGScheduler.scala:3910)
	at org.apache.spark.scheduler.DAGScheduler.$anonfun$abortStage$2$adapted(DAGScheduler.scala:3897)
	at scala.collection.mutable.ResizableArray.foreach(ResizableArray.scala:62)
	at scala.collection.mutable.ResizableArray.foreach$(ResizableArray.scala:55)
	at scala.collection.mutable.ArrayBuffer.foreach(ArrayBuffer.scala:49)
	at org.apache.spark.scheduler.DAGScheduler.abortStage(DAGScheduler.scala:3897)
	at org.apache.spark.scheduler.DAGScheduler.$anonfun$handleTaskSetFailed$1(DAGScheduler.scala:1758)
	at org.apache.spark.scheduler.DAGScheduler.$anonfun$handleTaskSetFailed$1$adapted(DAGScheduler.scala:1741)
	at scala.Option.foreach(Option.scala:407)
	at org.apache.spark.scheduler.DAGScheduler.handleTaskSetFailed(DAGScheduler.scala:1741)
	at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.doOnReceive(DAGScheduler.scala:4256)
	at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.onReceive(DAGScheduler.scala:4159)
	at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.onReceive(DAGScheduler.scala:4145)
	at org.apache.spark.util.EventLoop$$anon$1.run(EventLoop.scala:55)
Caused by: org.apache.spark.api.python.PythonException: Traceback (most recent call last):
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
TypeError: fetch_data_from_url() missing 1 required positional argument: 'page'

	at org.apache.spark.api.python.BasePythonRunner$ReaderIterator.handlePythonException(PythonRunner.scala:551)
	at org.apache.spark.sql.execution.python.BasePythonUDFRunner$$anon$2.read(PythonUDFRunner.scala:115)
	at org.apache.spark.sql.execution.python.BasePythonUDFRunner$$anon$2.read(PythonUDFRunner.scala:98)

	at org.apache.spark.storage.BlockManager.org$apache$spark$storage$BlockManager$$doPut(BlockManager.scala:1630)
	at org.apache.spark.storage.BlockManager.doPutIterator(BlockManager.scala:1695)
	
