"""
etl_job.py
~~~~~~~~~~

This Python module contains an example Apache Spark ETL job definition
that implements best practices for production ETL jobs. It can be
submitted to a Spark cluster (or locally) using the 'spark-submit'
command found in the '/bin' directory of all Spark distributions
(necessary for running any Spark job, locally or otherwise). For
example, this example script can be executed as follows,

    $SPARK_HOME/bin/spark-submit \
    --master spark://localhost:7077 \
    --py-files packages.zip \
    --files configs/etl_config.json \
    jobs/etl_job.py

where packages.zip contains Python modules required by ETL job (in
this example it contains a class to provide access to Spark's logger),
which need to be made available to each executor process on every node
in the cluster; etl_config.json is a text file sent to the cluster,
containing a JSON object with all of the configuration parameters
required by the ETL job; and, etl_job.py contains the Spark application
to be executed by a driver process on the Spark master node.

For more details on submitting Spark applications, please see here:
http://spark.apache.org/docs/latest/submitting-applications.html

Our chosen approach for structuring jobs is to separate the individual
'units' of ETL - the Extract, Transform and Load parts - into dedicated
functions, such that the key Transform steps can be covered by tests
and jobs or called from within another environment (e.g. a Jupyter or
Zeppelin notebook).
"""

from pyspark.sql import Row
from pyspark.sql.functions import col, concat_ws, lit

from dependencies.spark import start_spark
from dependencies.ima_file_reader import td_load_data
from dependencies.ima_file_writter import write_csv_data



def main():
    """Main ETL script definition.

    :return: None
    """
    # start Spark application and get Spark session, logger and config
    spark, log, config = start_spark(
        app_name='td_to_csv_loader'
        )

    # log that main ETL job is starting
    log.warn('etl_job is up-and-running')
    output_dir='result'
    username=config['username']
    password=config['password']
    sql="select * from sandbox.bia_usr_merc_ugc_intl sample 10"

    # execute ETL pipeline
    data = td_load_data(spark,username,password,sql)
    write_csv_data(data,output_dir)

    # log the success and terminate Spark application
    log.warn('td_to_csv_loader is finished')
    spark.stop()
    return None





# entry point for PySpark ETL application
if __name__ == '__main__':
    main()
