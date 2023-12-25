"""
    Requirement
    -------------------
        >> This program demonstrates how to merge updated data with the existing delta table.

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
    headers = ['country', 'year', 'temperature']

    # sample data to write
    json_data = [("Brazil", 2011, 22.029),
                 ("India", 2006, 24.73)]

    # Data to append with different schema
    json_delta_update = [("Australia", 2019, 24.0),
                         ("India", 2006, 25.05),
                         ("India", 2010, 27.05),
                         ("Brazil", 2013, 22.029)]

    # change this flag to choose between overwrite or append operations
    operation = 'create'
    #operation = 'merge'

    if operation == 'create':
        print('Overwrite : Creating & storing the data in delta table.')
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

    elif operation == 'merge':
        print('*************** Delta merge - appending new updates received')
        delta_table_merge = DeltaTable.forPath(spark, delta_table_path)

        print('************* Merge table:')
        delta_table_merge.toDF().show()

        delta_updates = spark.sparkContext \
            .parallelize(json_delta_update) \
            .toDF(headers)

        print('Delta updates to be merged - update/inserted:')
        delta_updates.show()

        # merge the data - update if record exist or insert it
        print("************* Merging them both for matching country and year.")
        delta_table_merge.alias('delta_merge') \
            .merge(source=delta_updates.alias('dt_updates'),
                   condition="delta_merge.country = dt_updates.country and delta_merge.year = dt_updates.year") \
            .whenMatchedUpdate(set={'temperature': "dt_updates.temperature"}) \
            .whenNotMatchedInsert(values={"country": "dt_updates.country", "year": "dt_updates.year", "temperature": "dt_updates.temperature"}) \
            .execute()

        delta_table_merge.toDF().show()
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
# operation = create
# ----------------------
# Overwrite : Creating & storing the data in delta table.
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
# -------------------------------------------
# operation = merge
# -----------------------
# *************** Delta merge - appending new updates received
# ************* Merge table:
# +-------+----+-----------+
# |country|year|temperature|
# +-------+----+-----------+
# | Brazil|2011|     22.029|
# |  India|2006|      24.73|
# +-------+----+-----------+
#
# Delta updates to be merged - update/inserted:
# +---------+----+-----------+
# |  country|year|temperature|
# +---------+----+-----------+
# |Australia|2019|       24.0|
# |    India|2006|      25.05|
# |    India|2010|      27.05|
# |   Brazil|2013|     22.029|
# +---------+----+-----------+
#
# ************* Merging them both for matching country and year.
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
#
#
