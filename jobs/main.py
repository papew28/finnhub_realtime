from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col, explode
from cassandra.cluster import Cluster
import json



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
    spark = SparkSession.builder \
                        .appName("finhub_realtime_streaming") \
                        .config("spark.jars.packages", 
                                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
                                "org.apache.hadoop:hadoop-client:3.4.0,"
                                "org.apache.spark:spark-avro_2.12:3.5.1,"
                                "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0"
                                ) \
                        .getOrCreate()
    return spark

#configuration de cassandra
def create_keyspace(session):
    session.execute("CREATE KEYSPACE IF NOT EXISTS finhub_streaming \
                     WITH REPLICATION= {'class': 'SimpleStrategy', 'replication_factor': 1};")


def create_table(session):
    session.execute("CREATE TABLE IF NOT EXISTS finhub_streaming.trading_data \
                     (symbol text, price float, timestamp timestamp, volume float, PRIMARY KEY (symbol, timestamp));")
    

def create_session():
    cluster = Cluster(['localhost'])
    session = cluster.connect()
    print("connected to cassandra")
    return session

def for_each_batch_function(df, epoch_id):
    
    print(f"writing data to cassandra for  {epoch_id}")
    df.write \
       .format("org.apache.spark.sql.cassandra") \
       .options(table="trading_data", keyspace="finhub_streaming") \
       .mode("append") \
       .save()

     

def main():
    spark = create_spark_session()
    
    kafka_stream = spark.readStream \
                       .format("kafka") \
                       .option("kafka.bootstrap.servers", "kafka_broker:29092") \
                       .option("subscribe", "trading_data") \
                       .load()
    
    kafka_stream_data = kafka_stream.selectExpr("CAST(value AS BINARY)")
    kafka_deserialized = kafka_stream_data.select(from_avro(col("value"), avro_schema).alias("data"))
    kafka_final_data = kafka_deserialized.select("data.*")
    trading_data =kafka_final_data.select(explode(col("data")).alias("data"))\
                               .select("data.*")
    

    trading_data = trading_data \
                  .withColumnRenamed("c", "trading condition") \
                  .withColumnRenamed("s", "symbol") \
                  .withColumnRenamed("p", "price") \
                  .withColumnRenamed("t", "timestamp") \
                  .withColumnRenamed("v", "volume")

    transformed_data = trading_data.withColumn(
    "timestamp",
   (col("timestamp") / 1000).cast("timestamp")
 )
        
    cassandra_session = create_session()
    create_keyspace(cassandra_session)
    create_table(cassandra_session)
    query = transformed_data.writeStream \
        .foreachBatch(for_each_batch_function) \
        .start()

    query.awaitTermination()

    cassandra_session.shutdown()

if __name__ == "__main__":
    main()
