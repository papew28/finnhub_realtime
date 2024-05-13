
import logging
from pyspark.sql import SparkSession

def get_spark_connection():
   
    conn=None
    
    try:
        conn = SparkSession.builder \
                  .appName('finhubrealtimestreaming') \
                  .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.13:3.5.0,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()

        conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return conn


def read_kafka_stream(spark, topic_name):
    try:
        df = spark.readStream.format("kafka") \
            .option("kafka.bootstrap.servers", "kafka_broker:29092") \
            .option("subscribe", topic_name) \
            .option("startingOffsets", "latest") \
            .load()
        df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

        logging.info(f"Reading data from kafka topic: {topic_name}")
    except Exception as e:
        logging.error(f"Couldn't read data from kafka topic due to exception {e}")
        return None

    return df     
    

