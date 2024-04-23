import logging
from datetime import datetime

from cassandra.auth import PlainTextAuthenticator
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType


def create_keyspace(session):
    """
    Create a Keyspace. Similar to creating a schema.

    IN:
        session
    OUT: 
        None
    """
    session.execute("""
                    CREATE KEYSPACE IF NOT EXISTS spark_streams
                    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
                    """)
    logging.info('Keyspace created successfully')


def create_table(session):
    session.execute("""
                    CREATE TABLE IF NOT EXISTS spark_streams.created_users(
                    id UUID PRIMARY KEY,
                    first_name TEXT,
                    last_name TEXT,
                    genre TEXT,
                    address TEXT,
                    post_code TEXT,
                    email TEXT,
                    username TEXT,
                    registered_date TEXT,
                    phone TEXT,
                    picture TEXT;
                    )
                """)
    logging.info('Table created successfully')


def insert_data(session, **kwargs):
    logging.info('Start inserting data...')
    user_id = kwargs['id']
    first_nanme = kwargs['first_name']
    last_name = kwargs['last_name']
    gender = kwargs['gender']
    address = kwargs['address']
    postcode = kwargs['postcode']
    email = kwargs['email']
    username = kwargs['username']
    dob = kwargs['dob']
    registered_date = kwargs['registered_date']
    phone = kwargs['phone']
    picture = kwargs['picture']

    try:
        session.execute("""
                INSERT INTO spark_streams.created_users(id, fist_name, last_name, gender, address,
                        postcode, email, username, dob, registered_date, phone, picture)
                        VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        ),"""
                        (user_id, first_nanme, last_name, gender, address,
                         postcode, email, username, dob, registered_date, phone, picture)
                        )
        logging.info(f"Data ingestion for {user_id} {first_nanme} {last_name}")

    except Exception as e:
        logging.error(
            f"An error has occured while inserting data {user_id} with exception {e}")


def create_spark_connection():
    s_conn = None
    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataSTreaming') \
            .config('spark.jars.packages', "com.datastax.spark.spark-cassandra-connector_2.13:3.41",
                    "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()

        s_conn.sparkContext.setlogLevel('ERROR')
        logging.info('Spark connection created successfully!')

    except Exception as e:
        logging.error(f'Could not create a spark connection du to: {e}')

    return s_conn


def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'user_created') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info('Initial kafka dataframe created succesfully')
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created due to {e}")

    return spark_df


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


def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField('id', StringType(), False),
        StructField('first_name', StringType(), False),
        StructField('last_name', StringType(), False),
        StructField('gender', StringType(), False),
        StructField('address', StringType(), False),
        StructField('postcode', StringType(), False),
        StructField('email', StringType(), False),
        StructField('username', StringType(), False),
        StructField('dob', StringType(), False),
        StructField('registered_date', StringType(), False),
        StructField('phone', StringType(), False),
        StructField('picture', StringType(), False)
    ])
    # select data from the kafka queue and format it into the schema define above
    sel = spark_df.selectExpr("CAST(value AS SRING)")\
        .select(from_json(col('value'), schema).alias('data')).select('data.*')
    print(sel)

    return sel


if __name__ == "__main__":
    # create spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # connect to kafka using spark connection
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)
        session = create_cassandra_connection()

        if session is not None:
            create_keyspace(session)
            create_table(session)
            # insert_data(session)
            # .format is for where we are going to write
            streaming_query = (selection_df.writeStream
                               .format('org.apache.spark.sql.cassandra')
                               .option('checkpointLocation', '/tmp/checkpoint')
                               .option('keyspace', 'spark_streams')
                               .option('tabble', 'created_users')
                               .start()
                               )
            streaming_query.awaitTermination()
