import logging
from datetime import datetime

from cassandra.auth import PlainTextAuthenticator
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col


def create_keyspace(session):
    """
    Create a Keyspace. Similar to creating a schema.

    IN:
        session:
    OUT:

    """
    pass


def create_table(session):
    pass


def insert_data(session, **kwargs):
    pass


def create_spark_connection():
    s_conn = None
    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataSTreaming') \
            .config('spark.jars.packages', "com.datastax.spark.spark-cassandra-connector_2.13:3.41",
                    "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1") \
            .config('spark.cassandra.onnection.host', 'localhost') \
            .getOrCreate()

        s_conn.sparkContext.setlogLevel('ERROR')
        logging.info('Spark connection created successfully!')

    except Exception as e:
        logging.error(f'Could not create a spark connection du to: {e}')

    return s_conn


def create_cassandra_connection():
    try:
        # Connecing to the cassanra cluster
        cluster = Cluster(['localhost'])

        cas_session = cluster.connect()
        return cas_session

    except Exception as e:
        logging.error(
            f"Impossible to connect to the cassandra cluster due to the following exception: {e}")
        return None


if __name__ == "__main__":
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        session = create_cassandra_connection()
        if session is not None:
            create_keyspace
