"""
    Requirement
    ---------------
        >> Demonstrate use case to read delta table files and then do record level update.


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
    #headers = ['country', 'year', 'temperature']

    # Read the delta table files from
    print(f'Reading data from : {delta_table_path}')
    delta_table = DeltaTable \
        .forPath(spark, path=delta_table_path)

    # print data details
    print(f'*********** delta_table.isDeltaTable : {delta_table.isDeltaTable}')
    print(f'*********** delta_table.detail() : {delta_table.detail()}')

    # data
    temp_df = delta_table.toDF()
    temp_df.printSchema()
    temp_df.show()

    # Update
    print("Updating 'temperature = 24.029' where country = 'Brazil'.")
    delta_table \
        .update("country = 'Brazil'", {"temperature": "24.029"})

    # Update recorded year for India from 2006 to 2010
    print("Update recorded year for India from 2006 to 2010.")
    delta_table \
        .update(condition="country = 'India'", set={'year': '2010'})

    # Write the updated data
    updated_delta_df = delta_table.toDF()

    print('Updated delta table.........')
    updated_delta_df.show(truncate=False)

    # Observation:
    # >> We don't need to write the updated delta table explicitly it automatically updated
    #    the source files in delta lake.
    #
    # Write updated data to delta table files
    # It will create a new parquet file will updated data
    # print('*********** Write starting.')
    # updated_delta_df \
    #     .coalesce(1) \
    #     .write \
    #     .format('delta') \
    #     .mode('append') \
    #     .save(delta_table_path)
    # print('*********** Write completed.')

    # Read data and print it
    print(f'************ Read from {delta_table_path} and show the records.')
    temp_table = DeltaTable.forPath(spark, delta_table_path)
    temp_table.toDF().show()

#
# Commands
# ---------------
# spark-submit --packages "org.apache.hadoop:hadoop-aws:2.7.4,io.delta:delta-core_2.11:0.6.0" --master yarn ./program.py
#
# Environment : DataBricks Community cluster, AWS S3
# ----------------
#
# Output
# --------------------
# Reading data from : s3a://vsingh-spark-apps-write-data/delta-lake/schema-enforcement-delta
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
# | Brazil|2011|       21.0|
# |  India|2006|      24.73|
# +-------+----+-----------+
#
# Updating 'temperature = 24.029' where country = 'Brazil' ..
# Update recorded year for India from 2006 to 2010.
# Updated delta table.........
# +-------+----+-----------+
# |country|year|temperature|
# +-------+----+-----------+
# | Brazil|2011|     24.029|
# |  India|2010|      24.73|
# +-------+----+-----------+
#
# ************ Read from s3a://vsingh-spark-apps-write-data/delta-lake/schema-enforcement-delta and show the records.
# +-------+----+-----------+
# |country|year|temperature|
# +-------+----+-----------+
# | Brazil|2011|     24.029|
# |  India|2010|      24.73|
# +-------+----+-----------+
#
#
