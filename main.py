from spark_job import get_spark_connection, read_kafka_stream


if __name__ == "__main__":
    spark = get_spark_connection()
    topic_name = 'trading_data'
    df = read_kafka_stream(spark, topic_name)
    