"""
    Requirement
    ---------------
        >> Demonstrate use case to check history/versions to debug
        >> and restore to specific version or timestamp.

"""

import os
import yaml
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

if __name__ == '__main__':

    # Read config files
    project_home = os.path.abspath(os.path.dirname(__file__)) + '/../../../'
    app_conf = yaml.load(open(os.path.abspath(project_home + 'application.yml')), Loader=yaml.FullLoader)
    secrets = yaml.load(open(os.path.abspath(project_home + '.secrets')), Loader=yaml.FullLoader)

    # Spark session
    AWS_CREDS = secrets['AWS_CREDS']
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

    # change the operation value to see all version or any specific version
    #operation = 'all_version'
    #operation = 'version'
    #operation = 'restoreToVersion'
    operation = 'restoreToTimestamp'

    if operation == 'all_version':
        print('*********** Showing all versions.')
        delta_table \
            .history() \
            .orderBy('version') \
            .show()

    elif operation == 'version':
        version = '1'
        print(f'*********** Showing version : {version}.')
        spark.read \
            .format('delta') \
            .option('versionAsOf', version) \
            .load(delta_table_path) \
            .show()

    elif operation == 'restoreToVersion':
        # check output of history API for more info
        delta_table.restoreToVersion(version=3)

    elif operation == 'restoreToTimestamp':
        # check output of history API for more info
        delta_table.restoreToTimestamp('2023-12-25 19:25:48')

    else:
        print(f'Operation : {operation} not supported.')


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
# operation = 'all_version'
# Reading data from : s3a://vsingh-spark-apps-write-data/delta-lake/schema-enforcement-delta
# *********** delta_table.isDeltaTable : <bound method DeltaTable.isDeltaTable of <class 'delta.tables.DeltaTable'>>
# *********** delta_table.detail() : DataFrame[format: string, id: string, name: string, description: string, location: string, createdAt: timestamp, lastModified: timestamp, partitionColumns: array<string>, numFiles: bigint, sizeInBytes: bigint, properties: map<string,string>, minReaderVersion: int, minWriterVersion: int, tableFeatures: array<string>, statistics: map<string,bigint>]
# root
#  |-- country: string (nullable = true)
#  |-- year: long (nullable = true)
#  |-- temperature: double (nullable = true)
#
# +---------+----+-----------+
# |  country|year|temperature|
# +---------+----+-----------+
# |Australia|2019|       24.0|
# |   Brazil|2011|     22.029|
# |   Brazil|2013|     22.029|
# |    India|2010|      27.05|
# +---------+----+-----------+
#
# *********** Showing all versions.
# +-------+-------------------+----------------+--------------------+---------+--------------------+----+------------------+--------------------+-----------+-----------------+-------------+--------------------+------------+--------------------+
# |version|          timestamp|          userId|            userName|operation| operationParameters| job|          notebook|           clusterId|readVersion|   isolationLevel|isBlindAppend|    operationMetrics|userMetadata|          engineInfo|
# +-------+-------------------+----------------+--------------------+---------+--------------------+----+------------------+--------------------+-----------+-----------------+-------------+--------------------+------------+--------------------+
# |      0|2023-12-25 19:10:53|2494341908896157|vs.rpc2021@outloo...|    WRITE|{mode -> Overwrit...|null|{3806004678516482}|1225-173922-5uztf23b|       null|WriteSerializable|        false|{numFiles -> 1, n...|        null|Databricks-Runtim...|
# |      1|2023-12-25 19:12:34|2494341908896157|vs.rpc2021@outloo...|    MERGE|{predicate -> ["(...|null|{3806004678516482}|1225-173922-5uztf23b|          0|WriteSerializable|        false|{numTargetRowsCop...|        null|Databricks-Runtim...|
# |      2|2023-12-25 19:25:48|2494341908896157|vs.rpc2021@outloo...|   DELETE|{predicate -> ["(...|null|{3806004678516484}|1225-173922-5uztf23b|          1|WriteSerializable|        false|{numRemovedFiles ...|        null|Databricks-Runtim...|
# |      3|2023-12-25 19:27:26|2494341908896157|vs.rpc2021@outloo...|   DELETE|{predicate -> ["(...|null|{3806004678516484}|1225-173922-5uztf23b|          2|WriteSerializable|        false|{numRemovedFiles ...|        null|Databricks-Runtim...|
# |      4|2023-12-25 19:48:35|2494341908896157|vs.rpc2021@outloo...|   DELETE|{predicate -> ["(...|null|{3806004678516484}|1225-173922-5uztf23b|          3|WriteSerializable|        false|{numRemovedFiles ...|        null|Databricks-Runtim...|
# +-------+-------------------+----------------+--------------------+---------+--------------------+----+------------------+--------------------+-----------+-----------------+-------------+--------------------+------------+--------------------+
#
# -------------------------------------
# operation = version
# --------------------------
# Reading data from : s3a://vsingh-spark-apps-write-data/delta-lake/schema-enforcement-delta
# *********** delta_table.isDeltaTable : <bound method DeltaTable.isDeltaTable of <class 'delta.tables.DeltaTable'>>
# *********** delta_table.detail() : DataFrame[format: string, id: string, name: string, description: string, location: string, createdAt: timestamp, lastModified: timestamp, partitionColumns: array<string>, numFiles: bigint, sizeInBytes: bigint, properties: map<string,string>, minReaderVersion: int, minWriterVersion: int, tableFeatures: array<string>, statistics: map<string,bigint>]
# root
#  |-- country: string (nullable = true)
#  |-- year: long (nullable = true)
#  |-- temperature: double (nullable = true)
#
# +---------+----+-----------+
# |  country|year|temperature|
# +---------+----+-----------+
# |Australia|2019|       24.0|
# |   Brazil|2011|     22.029|
# |   Brazil|2013|     22.029|
# |    India|2010|      27.05|
# +---------+----+-----------+
#
# *********** Showing version : 1.
# +---------+----+-----------+
# |  country|year|temperature|
# +---------+----+-----------+
# |Australia|2019|       24.0|
# |   Brazil|2011|     22.029|
# |   Brazil|2013|     22.029|
# |    India|2006|      25.05|
# |    India|2010|      27.05|
# +---------+----+-----------+
#
#
# -------------------------------------
# operation = restoreToVersion
# --------------------------
#
# Reading data from : s3a://vsingh-spark-apps-write-data/delta-lake/schema-enforcement-delta
# *********** delta_table.isDeltaTable : <bound method DeltaTable.isDeltaTable of <class 'delta.tables.DeltaTable'>>
# *********** delta_table.detail() : DataFrame[format: string, id: string, name: string, description: string, location: string, createdAt: timestamp, lastModified: timestamp, partitionColumns: array<string>, numFiles: bigint, sizeInBytes: bigint, properties: map<string,string>, minReaderVersion: int, minWriterVersion: int, tableFeatures: array<string>, statistics: map<string,bigint>]
# root
#  |-- country: string (nullable = true)
#  |-- year: long (nullable = true)
#  |-- temperature: double (nullable = true)
#
# +---------+----+-----------+
# |  country|year|temperature|
# +---------+----+-----------+
# |Australia|2019|       24.0|
# |   Brazil|2011|     22.029|
# |   Brazil|2013|     22.029|
# |    India|2010|      27.05|
# +---------+----+-----------+
#
# ---------------------------------------
# operation = 'restoreToTimestamp'
# ---------------------------------------
# Reading data from : s3a://vsingh-spark-apps-write-data/delta-lake/schema-enforcement-delta
# *********** delta_table.isDeltaTable : <bound method DeltaTable.isDeltaTable of <class 'delta.tables.DeltaTable'>>
# *********** delta_table.detail() : DataFrame[format: string, id: string, name: string, description: string, location: string, createdAt: timestamp, lastModified: timestamp, partitionColumns: array<string>, numFiles: bigint, sizeInBytes: bigint, properties: map<string,string>, minReaderVersion: int, minWriterVersion: int, tableFeatures: array<string>, statistics: map<string,bigint>]
# root
#  |-- country: string (nullable = true)
#  |-- year: long (nullable = true)
#  |-- temperature: double (nullable = true)
#
# +---------+----+-----------+
# |  country|year|temperature|
# +---------+----+-----------+
# |Australia|2019|       24.0|
# |   Brazil|2011|     22.029|
# |   Brazil|2013|     22.029|
# |    India|2010|      27.05|
# +---------+----+-----------+
#
#
