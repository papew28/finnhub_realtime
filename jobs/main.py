from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col, explode, array_join, when
from cassandra.policies import RetryPolicy, ConstantReconnectionPolicy
from cassandra.cluster import Cluster
import json
import time
from pyspark.sql.types import StringType


avro_schema=json.dumps({
    "namespace":"kafka.broker.producer",
    "type":"record",
    "name":"Stock",
    "fields":[
        {
            "name":"data",
            "type":{
                "type":"array",
                "items":{
                    "type":"record",
                    "name":"data",
                    "fields":[
                        {
                            "name": "c",
                            "type": [
                                {
                                    "type":"array",
                                    "items":["null","string"],
                                    "default":[]
                                },
                                "null"
                            ],
                            "doc":"les conditions de trading du symbole"
                        },
                        {
                            "name": "s",
                            "type": "string",
                            "doc":"symbol qui est tradé"
                        },
                        {
                            "name": "p",
                            "type": "float",
                            "description": "le prix du symbole"
                        },
                        {
                            "name": "t",
                            "type": "long",
                            "description": "le timestampe pour le trading du sysmbole"
                        },
                        {
                            "name": "v",
                            "type": "float",
                            "description": "le volume d'échange du symbole"
                        }
                    ]
                }
            }
        },
        {
            "name": "type",
            "type": "string",
            "doc": "Type of message"
        }     
    ],
    "doc":"les données de trading des symboles et le type"
}

)


def create_spark_session():
    try:
        spark = SparkSession.builder \
                        .appName("finhub_realtime_streaming") \
                        .config("spark.jars.packages", 
                                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
                                "org.apache.hadoop:hadoop-client:3.4.0,"
                                "org.apache.spark:spark-avro_2.12:3.5.1,"
                                "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0"
                                ) \
                        .config("spark.cassandra.connection.host", "cassandra") \
                        .getOrCreate()
        return spark
    except Exception as e:
        print(f"Erreur lors de la création de la session Spark: {str(e)}")
        raise

def create_session():
    max_retries = 5
    retry_delay = 5  
    attempt = 0
    
    while attempt < max_retries:
        try:
            cluster = Cluster(
                ['cassandra'],
                port=9042,
                protocol_version=4,
                connect_timeout=30,  
                control_connection_timeout=30,
                reconnection_policy=ConstantReconnectionPolicy(delay=5.0),
                idle_heartbeat_interval=30
            )
            
            session = cluster.connect(wait_for_all_pools=True)
            session.default_timeout = 30  
            print("Connected to Cassandra successfully")
            return session
            
        except Exception as e:
            attempt += 1
            if attempt == max_retries:
                print(f"Failed to connect to Cassandra after {max_retries} attempts: {str(e)}")
                raise
            print(f"Connection attempt {attempt} failed, retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)

def create_keyspace(session):
    try:
        session.execute(
            """
            CREATE KEYSPACE IF NOT EXISTS finhub_streaming 
            WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
            """,
            timeout=30
        )
        print("Keyspace created successfully")
    except Exception as e:
        print(f"Error creating keyspace: {str(e)}")
        raise

def create_table(session):
    try:
        session.execute(
            """
            CREATE TABLE IF NOT EXISTS finhub_streaming.trading_data
            (symbol text, price float, timestamp timestamp, volume float,
            trading_condition text, PRIMARY KEY (symbol, timestamp))
            """,
            timeout=30
        )
        print("Table created successfully")
    except Exception as e:
        print(f"Error creating table: {str(e)}")
        raise

def for_each_batch_function(df, epoch_id):
    max_retries = 3
    retry_delay = 5
    attempt = 0
    
    while attempt < max_retries:
        try:
            print(f"Writing batch {epoch_id} to Cassandra")
            df.write \
              .format("org.apache.spark.sql.cassandra") \
              .options(table="trading_data", keyspace="finhub_streaming") \
              .option("spark.cassandra.connection.timeout_ms", "30000") \
              .option("spark.cassandra.read.timeout_ms", "30000") \
              .mode("append") \
              .save()
            print(f"Successfully wrote batch {epoch_id}")
            break
            
        except Exception as e:
            attempt += 1
            if attempt == max_retries:
                print(f"Failed to write batch {epoch_id} after {max_retries} attempts: {str(e)}")
                raise
            print(f"Write attempt {attempt} failed, retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)

def process_trading_data(trading_data):
    
    processed_data = trading_data \
        .withColumn(
            "trading_condition",
            when(col("c").isNull(), None)
            .otherwise(array_join(col("c"), ","))
        ) \
        .withColumnRenamed("s", "symbol") \
        .withColumnRenamed("p", "price") \
        .withColumnRenamed("t", "timestamp") \
        .withColumnRenamed("v", "volume") \
        .drop("c")  

    processed_data = processed_data.withColumn(
        "timestamp",
        (col("timestamp") / 1000).cast("timestamp")
    )
    
    return processed_data


def main():
    try:
        spark = create_spark_session()
        print(f"Spark session created: {spark}")
        
       
        spark.conf.set("spark.cassandra.connection.timeout_ms", "30000")
        spark.conf.set("spark.cassandra.read.timeout_ms", "30000")

        kafka_stream = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka_broker:29092") \
            .option("subscribe", "trading_data") \
            .option("failOnDataLoss", "false") \
            .load()

        
        kafka_stream_data = kafka_stream.selectExpr("CAST(value AS BINARY)")
        kafka_deserialized = kafka_stream_data.select(
            from_avro(col("value"), avro_schema).alias("data")
        )
        kafka_final_data = kafka_deserialized.select("data.*")
        
        trading_data = kafka_final_data.select(
            explode(col("data")).alias("data")
        ).select("data.*")
        
    
        transformed_data = process_trading_data(trading_data)


        cassandra_session = create_session()
        create_keyspace(cassandra_session)
        create_table(cassandra_session)


        query = transformed_data.writeStream \
            .foreachBatch(for_each_batch_function) \
            .outputMode("append") \
            .start()
        
        query.awaitTermination()
        
         
    except Exception as e:
        print(f"Error in main program: {str(e)}")
        raise
    finally:
        if 'cassandra_session' in locals():
            try:
                cassandra_session.shutdown()
            except Exception as e:
                print(f"Error during Cassandra session shutdown: {str(e)}")

if __name__ == "__main__":
    main()

