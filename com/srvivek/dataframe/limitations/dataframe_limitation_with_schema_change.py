"""
    Requirement
    ----------------
        >> This program demonstrates the standard use case of Spark DataFrame limitation
           when writing delta update to existing file with different/modified schema.
"""

from pyspark.sql import SparkSession
import os, yaml


if __name__ == '__main__':

    spark = SparkSession.builder \
        .appName('Schema change limitation') \
        .getOrCreate()

    spark.sparkContext.setLogLevel('ERROR')

    # Read config files
    cur_dir = os.path.abspath(os.path.dirname(__file__))
    app_conf = yaml.load(open(os.path.abspath(cur_dir + '/../../../' + 'application.yml')), Loader=yaml.FullLoader)
    secrets = yaml.load(open(os.path.abspath(cur_dir + '/../../../' + '.secrets')), Loader=yaml.FullLoader)

    hdp_conf = spark.sparkContext._jsc.hadoopConfiguration()
    AWS_CREDS = secrets['AWS_CREDS']
    hdp_conf.set('fs.s3a.access.key', AWS_CREDS['ACCESS_KEY'])
    hdp_conf.set('fs.s3a.secret.key', AWS_CREDS['SECRET_KEY'])

    AWS_S3 = app_conf['AWS_S3']
    parquet_file_loc = 's3a://' + AWS_S3['DATA_BUCKET'] + '/' + AWS_S3['FILE_LOCATION']

    # Data headers
    headers = ['country', 'year', 'temperature']

    # sample data to write
    json_data = [("Brazil",  2011, 22.029),
                 ("India", 2006, 24.73)]

    # Data to append with different schema
    json_data_different_schema = [("Australia", 2019.0, 30.0)]

    # change this flag to choose between overwrite or append operations
    operation = "overwrite"
    if operation == 'overwrite':
        print('Overwrite : rewriting the data in parquet.')
        data_df = spark.sparkContext \
            .parallelize(json_data) \
            .toDF(headers)

        data_df.printSchema()
        data_df.show()

        # Write the data to lake
        print(f'*********** Writing data to : {parquet_file_loc}')
        data_df.coalesce(1) \
            .write \
            .format('parquet') \
            .mode('overwrite') \
            .save(parquet_file_loc)
        print('*********** Write completed.')

        print(f'*********** Reading data from : {parquet_file_loc}')
        temp_df = spark.read.parquet(parquet_file_loc)
        temp_df.printSchema()
        temp_df.show()

    elif operation == 'append':
        print('*************** Delta update - appending new updates to the parquet')
        data_df = spark.sparkContext \
            .parallelize(json_data_different_schema) \
            .toDF(headers)

        data_df.printSchema()
        data_df.show()

        # append into existing parquet file
        data_df.coalesce(1) \
            .write \
            .mode('append') \
            .parquet(parquet_file_loc)
        print('*********** Write completed.')

        print(f'*********** Reading data from : {parquet_file_loc}')
        temp_df = spark.read.parquet(parquet_file_loc)
        temp_df.printSchema()
        temp_df.show()

    else:
        print(f'Not supported operation : {operation}')


#
# command
# --------------
# Environment : AWS EMR, S3
#
# Output
# -------------
# Run -1 [operation = overwrite]
# ------------------------------------
# Overwrite : rewriting the data in parquet.
# root
#  |-- country: string (nullable = true)
#  |-- year: long (nullable = true)
#  |-- temperature: double (nullable = true)
#
# +-------+----+-----------+
# |country|year|temperature|
# +-------+----+-----------+
# | Brazil|2011|     22.029|
# |  India|2006|      24.73|
# +-------+----+-----------+
#
# *********** Writing data to : s3a://vsingh-spark-apps-write-data/delta-lake/schema-mismatch
# *********** Write completed.
# *********** Reading data from : s3a://vsingh-spark-apps-write-data/delta-lake/schema-mismatch
# root
#  |-- country: string (nullable = true)
#  |-- year: long (nullable = true)
#  |-- temperature: double (nullable = true)
#
# +-------+----+-----------+
# |country|year|temperature|
# +-------+----+-----------+
# | Brazil|2011|     22.029|
# |  India|2006|      24.73|
# +-------+----+-----------+
#
# ------------------------------------
# Run -1 [operation = append]
# ------------------------------------
# *************** Delta update - appending new updates to the parquet
# root
#  |-- country: string (nullable = true)
#  |-- year: double (nullable = true)
#  |-- temperature: double (nullable = true)
#
# +---------+------+-----------+
# |  country|  year|temperature|
# +---------+------+-----------+
# |Australia|2019.0|       30.0|
# +---------+------+-----------+
#
# *********** Write completed.
# *********** Reading data from : s3a://vsingh-spark-apps-write-data/delta-lake/schema-mismatch
# root
#  |-- country: string (nullable = true)
#  |-- year: long (nullable = true)
#  |-- temperature: double (nullable = true)
#
# Unexpected exception formatting exception. Falling back to standard exception
# Py4JJavaError: An error occurred while calling o784.showString.
# : org.apache.spark.SparkException: Job aborted due to stage failure: Task 0 in stage 33.0 failed 1 times, most recent failure: Lost task 0.0 in stage 33.0 (TID 69) (ip-10-172-162-18.us-west-2.compute.internal executor driver): com.databricks.sql.io.FileReadException: Error while reading file s3a://vsingh-spark-apps-write-data/delta-lake/schema-mismatch/part-00000-tid-3706729912313671596-1112aeea-9367-4d16-b216-aea11daf1413-67-1-c000.snappy.parquet. Parquet column cannot be converted. Column: [year], Expected: LongType, Found: DOUBLE
# 	at org.apache.spark.sql.execution.datasources.FileScanRDD$$anon$1$$anon$2.logFileNameAndThrow(FileScanRDD.scala:693)
# 	at org.apache.spark.sql.execution.datasources.FileScanRDD$$anon$1$$anon$2.getNext(FileScanRDD.scala:653)
# 	at org.apache.spark.util.NextIterator.hasNext(NextIterator.scala:73)
# 	at org.apache.spark.sql.execution.datasources.FileScanRDD$$anon$1.$anonfun$prepareNextFile$1(FileScanRDD.scala:872)
# 	at scala.concurrent.Future$.$anonfun$apply$1(Future.scala:659)
# 	at scala.util.Success.$anonfun$map$1(Try.scala:255)
# 	at scala.util.Success.map(Try.scala:213)
# 	at scala.concurrent.Future.$anonfun$map$1(Future.scala:292)
# 	at scala.concurrent.impl.Promise.liftedTree1$1(Promise.scala:33)
# 	at scala.concurrent.impl.Promise.$anonfun$transform$1(Promise.scala:33)
# 	at scala.concurrent.impl.CallbackRunnable.run(Promise.scala:64)
# 	at org.apache.spark.util.threads.SparkThreadLocalCapturingRunnable.$anonfun$run$1(SparkThreadLocalForwardingThreadPoolExecutor.scala:116)
# 	at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)
# 	at com.databricks.spark.util.IdentityClaim$.withClaim(IdentityClaim.scala:48)
# 	at org.apache.spark.util.threads.SparkThreadLocalCapturingHelper.$anonfun$runWithCaptured$4(SparkThreadLocalForwardingThreadPoolExecutor.scala:79)
# 	at com.databricks.unity.UCSEphemeralState$Handle.runWith(UCSEphemeralState.scala:41)
# 	at org.apache.spark.util.threads.SparkThreadLocalCapturingHelper.runWithCaptured(SparkThreadLocalForwardingThreadPoolExecutor.scala:78)
# 	at org.apache.spark.util.threads.SparkThreadLocalCapturingHelper.runWithCaptured$(SparkThreadLocalForwardingThreadPoolExecutor.scala:64)
# 	at org.apache.spark.util.threads.SparkThreadLocalCapturingRunnable.runWithCaptured(SparkThreadLocalForwardingThreadPoolExecutor.scala:113)
# 	at org.apache.spark.util.threads.SparkThreadLocalCapturingRunnable.run(SparkThreadLocalForwardingThreadPoolExecutor.scala:116)
# 	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
# 	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
# 	at java.lang.Thread.run(Thread.java:750)
# Caused by: org.apache.spark.sql.execution.datasources.SchemaColumnConvertNotSupportedException: column: [year], physicalType: DOUBLE, logicalType: LongType
# 	at com.databricks.sql.io.parquet.VectorizedColumnReader.constructConvertNotSupportedException(VectorizedColumnReader.java:582)
# 	at com.databricks.sql.io.parquet.VectorizedColumnReader.readDoubleBatch(VectorizedColumnReader.java:1026)
# 	at com.databricks.sql.io.parquet.VectorizedColumnReader.readBatch(VectorizedColumnReader.java:537)
# 	at com.databricks.sql.io.parquet.DatabricksVectorizedParquetRecordReader.nextBatch(DatabricksVectorizedParquetRecordReader.java:609)
# 	at org.apache.spark.sql.execution.datasources.parquet.VectorizedParquetRecordReader.nextKeyValue(VectorizedParquetRecordReader.java:256)
# 	at org.apache.spark.sql.execution.datasources.RecordReaderIterator.hasNext(RecordReaderIterator.scala:41)
# 	at org.apache.spark.sql.execution.datasources.FileScanRDD$$anon$1$$anon$2.getNext(FileScanRDD.scala:608)
# 	... 21 more
#
# Driver stacktrace:
# 	at org.apache.spark.scheduler.DAGScheduler.failJobAndIndependentStages(DAGScheduler.scala:3396)
# 	at org.apache.spark.scheduler.DAGScheduler.$anonfun$abortStage$2(DAGScheduler.scala:3327)
# 	at org.apache.spark.scheduler.DAGScheduler.$anonfun$abortStage$2$adapted(DAGScheduler.scala:3316)
# 	at scala.collection.mutable.ResizableArray.foreach(ResizableArray.scala:62)
# 	at scala.collection.mutable.ResizableArray.foreach$(ResizableArray.scala:55)
# 	at scala.collection.mutable.ArrayBuffer.foreach(ArrayBuffer.scala:49)
# 	at org.apache.spark.scheduler.DAGScheduler.abortStage(DAGScheduler.scala:3316)
# 	at org.apache.spark.scheduler.DAGScheduler.$anonfun$handleTaskSetFailed$1(DAGScheduler.scala:1433)
# 	at org.apache.spark.scheduler.DAGScheduler.$anonfun$handleTaskSetFailed$1$adapted(DAGScheduler.scala:1433)
# 	at scala.Option.foreach(Option.scala:407)
# 	at org.apache.spark.scheduler.DAGScheduler.handleTaskSetFailed(DAGScheduler.scala:1433)
# 	at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.doOnReceive(DAGScheduler.scala:3609)
# 	at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.onReceive(DAGScheduler.scala:3547)
# 	at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.onReceive(DAGScheduler.scala:3535)
# 	at org.apache.spark.util.EventLoop$$anon$1.run(EventLoop.scala:51)
# 	at org.apache.spark.scheduler.DAGScheduler.$anonfun$runJob$1(DAGScheduler.scala:1182)
# 	at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)
# 	at com.databricks.spark.util.FrameProfiler$.record(FrameProfiler.scala:80)
# 	at org.apache.spark.scheduler.DAGScheduler.runJob(DAGScheduler.scala:1170)
# 	at org.apache.spark.SparkContext.runJobInternal(SparkContext.scala:2750)
# 	at org.apache.spark.sql.execution.collect.Collector.$anonfun$runSparkJobs$1(Collector.scala:349)
# 	at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)
# 	at com.databricks.spark.util.FrameProfiler$.record(FrameProfiler.scala:80)
# 	at org.apache.spark.sql.execution.collect.Collector.runSparkJobs(Collector.scala:293)
# 	at org.apache.spark.sql.execution.collect.Collector.collect(Collector.scala:377)
# 	at org.apache.spark.sql.execution.collect.Collector$.collect(Collector.scala:128)
# 	at org.apache.spark.sql.execution.collect.Collector$.collect(Collector.scala:135)
# 	at org.apache.spark.sql.execution.qrc.InternalRowFormat$.collect(cachedSparkResults.scala:122)
# 	at org.apache.spark.sql.execution.qrc.InternalRowFormat$.collect(cachedSparkResults.scala:110)
# 	at org.apache.spark.sql.execution.qrc.InternalRowFormat$.collect(cachedSparkResults.scala:92)
# 	at org.apache.spark.sql.execution.qrc.ResultCacheManager.$anonfun$computeResult$1(ResultCacheManager.scala:537)
# 	at com.databricks.spark.util.FrameProfiler$.record(FrameProfiler.scala:80)
# 	at org.apache.spark.sql.execution.qrc.ResultCacheManager.collectResult$1(ResultCacheManager.scala:529)
# 	at org.apache.spark.sql.execution.qrc.ResultCacheManager.computeResult(ResultCacheManager.scala:549)
# 	at org.apache.spark.sql.execution.qrc.ResultCacheManager.$anonfun$getOrComputeResultInternal$1(ResultCacheManager.scala:402)
# 	at scala.Option.getOrElse(Option.scala:189)
# 	at org.apache.spark.sql.execution.qrc.ResultCacheManager.getOrComputeResultInternal(ResultCacheManager.scala:395)
# 	at org.apache.spark.sql.execution.qrc.ResultCacheManager.getOrComputeResult(ResultCacheManager.scala:289)
# 	at org.apache.spark.sql.execution.SparkPlan.$anonfun$executeCollectResult$1(SparkPlan.scala:506)
# 	at com.databricks.spark.util.FrameProfiler$.record(FrameProfiler.scala:80)
# 	at org.apache.spark.sql.execution.SparkPlan.executeCollectResult(SparkPlan.scala:503)
# 	at org.apache.spark.sql.Dataset.collectResult(Dataset.scala:3458)
# 	at org.apache.spark.sql.Dataset.collectFromPlan(Dataset.scala:4382)
# 	at org.apache.spark.sql.Dataset.$anonfun$head$1(Dataset.scala:3168)
# 	at org.apache.spark.sql.Dataset.$anonfun$withAction$3(Dataset.scala:4373)
# 	at org.apache.spark.sql.execution.QueryExecution$.withInternalError(QueryExecution.scala:819)
# 	at org.apache.spark.sql.Dataset.$anonfun$withAction$2(Dataset.scala:4371)
# 	at org.apache.spark.sql.execution.SQLExecution$.$anonfun$withCustomExecutionEnv$8(SQLExecution.scala:233)
# 	at org.apache.spark.sql.execution.SQLExecution$.withSQLConfPropagated(SQLExecution.scala:417)
# 	at org.apache.spark.sql.execution.SQLExecution$.$anonfun$withCustomExecutionEnv$1(SQLExecution.scala:178)
# 	at org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:1038)
# 	at org.apache.spark.sql.execution.SQLExecution$.withCustomExecutionEnv(SQLExecution.scala:128)
# 	at org.apache.spark.sql.execution.SQLExecution$.withNewExecutionId(SQLExecution.scala:367)
# 	at org.apache.spark.sql.Dataset.withAction(Dataset.scala:4371)
# 	at org.apache.spark.sql.Dataset.head(Dataset.scala:3168)
# 	at org.apache.spark.sql.Dataset.take(Dataset.scala:3389)
# 	at org.apache.spark.sql.Dataset.getRows(Dataset.scala:315)
# 	at org.apache.spark.sql.Dataset.showString(Dataset.scala:354)
# 	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
# 	at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
# 	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
# 	at java.lang.reflect.Method.invoke(Method.java:498)
# 	at py4j.reflection.MethodInvoker.invoke(MethodInvoker.java:244)
# 	at py4j.reflection.ReflectionEngine.invoke(ReflectionEngine.java:380)
# 	at py4j.Gateway.invoke(Gateway.java:306)
# 	at py4j.commands.AbstractCommand.invokeMethod(AbstractCommand.java:132)
# 	at py4j.commands.CallCommand.execute(CallCommand.java:79)
# 	at py4j.ClientServerConnection.waitForCommands(ClientServerConnection.java:195)
# 	at py4j.ClientServerConnection.run(ClientServerConnection.java:115)
# 	at java.lang.Thread.run(Thread.java:750)
# Caused by: com.databricks.sql.io.FileReadException: Error while reading file s3a://vsingh-spark-apps-write-data/delta-lake/schema-mismatch/part-00000-tid-3706729912313671596-1112aeea-9367-4d16-b216-aea11daf1413-67-1-c000.snappy.parquet. Parquet column cannot be converted. Column: [year], Expected: LongType, Found: DOUBLE
# 	at org.apache.spark.sql.execution.datasources.FileScanRDD$$anon$1$$anon$2.logFileNameAndThrow(FileScanRDD.scala:693)
# 	at org.apache.spark.sql.execution.datasources.FileScanRDD$$anon$1$$anon$2.getNext(FileScanRDD.scala:653)
# 	at org.apache.spark.util.NextIterator.hasNext(NextIterator.scala:73)
# 	at org.apache.spark.sql.execution.datasources.FileScanRDD$$anon$1.$anonfun$prepareNextFile$1(FileScanRDD.scala:872)
# 	at scala.concurrent.Future$.$anonfun$apply$1(Future.scala:659)
# 	at scala.util.Success.$anonfun$map$1(Try.scala:255)
# 	at scala.util.Success.map(Try.scala:213)
# 	at scala.concurrent.Future.$anonfun$map$1(Future.scala:292)
# 	at scala.concurrent.impl.Promise.liftedTree1$1(Promise.scala:33)
# 	at scala.concurrent.impl.Promise.$anonfun$transform$1(Promise.scala:33)
# 	at scala.concurrent.impl.CallbackRunnable.run(Promise.scala:64)
# 	at org.apache.spark.util.threads.SparkThreadLocalCapturingRunnable.$anonfun$run$1(SparkThreadLocalForwardingThreadPoolExecutor.scala:116)
# 	at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)
# 	at com.databricks.spark.util.IdentityClaim$.withClaim(IdentityClaim.scala:48)
# 	at org.apache.spark.util.threads.SparkThreadLocalCapturingHelper.$anonfun$runWithCaptured$4(SparkThreadLocalForwardingThreadPoolExecutor.scala:79)
# 	at com.databricks.unity.UCSEphemeralState$Handle.runWith(UCSEphemeralState.scala:41)
# 	at org.apache.spark.util.threads.SparkThreadLocalCapturingHelper.runWithCaptured(SparkThreadLocalForwardingThreadPoolExecutor.scala:78)
# 	at org.apache.spark.util.threads.SparkThreadLocalCapturingHelper.runWithCaptured$(SparkThreadLocalForwardingThreadPoolExecutor.scala:64)
# 	at org.apache.spark.util.threads.SparkThreadLocalCapturingRunnable.runWithCaptured(SparkThreadLocalForwardingThreadPoolExecutor.scala:113)
# 	at org.apache.spark.util.threads.SparkThreadLocalCapturingRunnable.run(SparkThreadLocalForwardingThreadPoolExecutor.scala:116)
# 	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
# 	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
# 	... 1 more
# Caused by: org.apache.spark.sql.execution.datasources.SchemaColumnConvertNotSupportedException: column: [year], physicalType: DOUBLE, logicalType: LongType
# 	at com.databricks.sql.io.parquet.VectorizedColumnReader.constructConvertNotSupportedException(VectorizedColumnReader.java:582)
# 	at com.databricks.sql.io.parquet.VectorizedColumnReader.readDoubleBatch(VectorizedColumnReader.java:1026)
# 	at com.databricks.sql.io.parquet.VectorizedColumnReader.readBatch(VectorizedColumnReader.java:537)
# 	at com.databricks.sql.io.parquet.DatabricksVectorizedParquetRecordReader.nextBatch(DatabricksVectorizedParquetRecordReader.java:609)
# 	at org.apache.spark.sql.execution.datasources.parquet.VectorizedParquetRecordReader.nextKeyValue(VectorizedParquetRecordReader.java:256)
# 	at org.apache.spark.sql.execution.datasources.RecordReaderIterator.hasNext(RecordReaderIterator.scala:41)
# 	at org.apache.spark.sql.execution.datasources.FileScanRDD$$anon$1$$anon$2.getNext(FileScanRDD.scala:608)
#
#
