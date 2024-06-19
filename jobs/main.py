from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col
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
                                "org.apache.spark:spark-avro_2.12:3.5.1"
                                ) \
                        .getOrCreate()
    return spark

def create_cassandra_keyspace():

    pass

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
    query = kafka_final_data \
            .writeStream \
            .outputMode("append") \
            .format("console") \
            .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
