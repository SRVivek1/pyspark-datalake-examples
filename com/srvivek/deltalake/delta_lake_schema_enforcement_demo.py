"""
    Requirement
    -------------------
        >> This program demonstrates the delta lake schema enforcement feature using delta
           format/table instead of parquet files.
        >> How data lake schema enforcement validates schema on write.

    requirement.txt
    --------------------
        >> ## delta lake support
            1. delta
            2. delta-spark
            3. PyYAML~=6.0.1
"""

import os
import yaml
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

if __name__ == '__main__':

    # Read config files
    cur_dir = os.path.abspath(os.path.dirname(__file__))
    app_conf = yaml.load(open(os.path.abspath(cur_dir + '/../../../' + 'application.yml')), Loader=yaml.FullLoader)
    secrets = yaml.load(open(os.path.abspath(cur_dir + '/../../../' + '.secrets')), Loader=yaml.FullLoader)

    AWS_CREDS = secrets['AWS_CREDS']
    # Spark session
    lib_dependencies = 'org.apache.hadoop:hadoop-aws:2.7.4,io.delta:delta-core_2.11:0.6.0'
    spark = SparkSession.builder \
        .appName('Delta lake schema enforcement') \
        .config('spark.jars.packages', lib_dependencies) \
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
        .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.hadoop.fs.s3a.access.key", AWS_CREDS['ACCESS_KEY']) \
        .config("spark.hadoop.fs.s3a.secret.key", AWS_CREDS['SECRET_KEY']) \
        .getOrCreate()

    spark.sparkContext.setLogLevel('ERROR')

    hdp_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hdp_conf.set('fs.s3a.access.key', AWS_CREDS['ACCESS_KEY'])
    hdp_conf.set('fs.s3a.secret.key', AWS_CREDS['SECRET_KEY'])

    AWS_S3 = app_conf['AWS_S3']
    delta_table_path = 's3a://' + AWS_S3['DATA_BUCKET'] + '/' + AWS_S3['FILE_LOCATION_DELTA']

    # Data headers
    headers = ['country', 'year', 'temperature']

    # sample data to write
    json_data = [("Brazil", 2011, 22.029),
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
        print(f'*********** Writing data to : {delta_table_path}')
        data_df.coalesce(1) \
            .write \
            .format('delta') \
            .mode('overwrite') \
            .save(delta_table_path)
        print('*********** Write completed.')

        print(f'*********** Reading data from delta file : {delta_table_path}')
        delta_table = DeltaTable \
            .forPath(spark, delta_table_path)
        print(f'*********** delta_table.isDeltaTable : {delta_table.isDeltaTable}')
        print(f'*********** delta_table.detail() : {delta_table.detail()}')

        temp_df = delta_table.toDF()
        temp_df.printSchema()
        temp_df.show()

    elif operation == 'append':
        print('*************** Delta update - appending new updates to the parquet')
        data_df = spark.sparkContext \
            .parallelize(json_data_different_schema) \
            .toDF(headers)

        data_df.printSchema()
        data_df.show()

        # append into existing delta table (parquet) file
        data_df.coalesce(1) \
            .write \
            .format('delta') \
            .mode('append') \
            .save(delta_table_path)
        print('*********** Write completed.')

        print(f'*********** Reading data from : {delta_table_path}')
        delta_table = DeltaTable \
            .forPath(spark, delta_table_path)
        print(f'*********** delta_table.isDeltaTable : {delta_table.isDeltaTable}')
        print(f'*********** delta_table.detail() : {delta_table.detail()}')

        temp_df = delta_table.toDF()
        temp_df.printSchema()
        temp_df.show()

    else:
        print(f'Not supported operation : {operation}')

#
# command
# --------------
# spark-submit --packages "org.apache.hadoop:hadoop-aws:2.7.4,io.delta:delta-core_2.11:0.6.0" --master yarn ./program.py
# Environment :
#   --> DataBricks Community server,
#   --> AWS S3
#
# Output
# -------------
# Run - 1 [operation : overwrite]
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
# *********** Writing data to : s3a://vsingh-spark-apps-write-data/delta-lake/schema-enforcement-delta
# *********** Write completed.
# *********** Reading data from delta file : s3a://vsingh-spark-apps-write-data/delta-lake/schema-enforcement-delta
# *********** delta_table.isDeltaTable : <bound method DeltaTable.isDeltaTable of <class 'delta.tables.DeltaTable'>>
# *********** delta_table.detail() : DataFrame[format: string, id: string, name: string, description: string, location: string, createdAt: timestamp, lastModified: timestamp, partitionColumns: array<string>, numFiles: bigint, sizeInBytes: bigint, properties: map<string,string>, minReaderVersion: int, minWriterVersion: int, tableFeatures: array<string>, statistics: map<string,bigint>]
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
#
# #
