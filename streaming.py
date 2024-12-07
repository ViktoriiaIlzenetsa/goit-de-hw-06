from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType
from pyspark.sql import SparkSession
from configs import kafka_config
import uuid
import os

# Пакет, необхідний для читання Kafka зі Spark
os.environ[
    'PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell'

# Створення SparkSession
spark = (SparkSession.builder
         .appName("KafkaStreaming")
         .master("local[*]")
         .getOrCreate())

# Читання потоку даних із Kafka
# Вказівки, як саме ми будемо під'єднуватися, паролі, протоколи
# maxOffsetsPerTrigger - будемо читати 5 записів за 1 тригер.
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'][0]) \
    .option("kafka.security.protocol", "SASL_PLAINTEXT") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config",
            'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="VawEzo1ikLtrA8Ug8THa";') \
    .option("subscribe", "viktoriia_streaming_building_sensors") \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", "5") \
    .load()

# Визначення схеми для JSON,
# оскільки Kafka має структуру ключ-значення, а значення має формат JSON. 
json_schema = StructType([
    StructField("sensor_id", IntegerType(), True),
    StructField("timestamp", StringType(), True),
    StructField("temperature", IntegerType(), True),
    StructField("humidity", IntegerType(), True),
   # StructField("value", IntegerType(), True)
])



# Маніпуляції з даними
clean_df = df.selectExpr("CAST(key AS STRING) AS key_deserialized", "CAST(value AS STRING) AS value_deserialized", "*") \
    .drop('key', 'value') \
    .withColumnRenamed("key_deserialized", "key") \
    .withColumn("value_json", from_json(col("value_deserialized"), json_schema)) \
    .withColumn("timestamp", from_unixtime(col("value_json.timestamp").cast(DoubleType())).cast("timestamp")) \
    .withColumn("temperature", col("value_json.temperature")) \
    .withColumn("humidity", col("value_json.humidity")) \
    .drop("value_json", "value_deserialized", "offset", "topic", "key", "partition", "timestampType") \


df_with_window = clean_df.withWatermark("timestamp", "10 seconds") \
    .groupBy(window("timestamp", "1 minutes", "30 seconds")) \
    .agg(
        avg("temperature").alias("avg_t"),
        avg("humidity").alias("avg_h")
    )

alerts_conditions = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv('alerts_conditions.csv')

df_cross = df_with_window.crossJoin(alerts_conditions)\
    .filter(((col("avg_h") >= col("humidity_min")) & ((col("avg_h") < col("humidity_max")))) | ((col("avg_t") >= col("temperature_min")) & ((col("avg_t") < col("temperature_max")))))\
    .withColumn("timestamp", current_timestamp())

# Виведення даних на екран
# displaying_df = df_cross.writeStream \
#     .trigger(availableNow=True) \
#     .outputMode("append") \
#     .format("console") \
#     .option("checkpointLocation", "HW06/checkpoints") \
#     .start() \
#     .awaitTermination()


# Підготовка даних для запису в Kafka: формування ключ-значення
prepare_to_kafka_df = df_cross.select(
    to_json(struct(col("window"), col("avg_t"),
    col("avg_h"), col("code"), col("message"), col("timestamp"))).alias("value")
    )


#Запис оброблених даних у Kafka-топік 'oleksiy_spark_streaming_out'
query = prepare_to_kafka_df.selectExpr("CAST(123 AS STRING) AS key", "CAST(value AS STRING) AS value") \
    .writeStream \
    .trigger(processingTime='5 seconds') \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "77.81.230.104:9092") \
    .option("topic", "viktoriia_streaming_alerts") \
    .option("kafka.security.protocol", "SASL_PLAINTEXT") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config",
            "org.apache.kafka.common.security.plain.PlainLoginModule required username='admin' password='VawEzo1ikLtrA8Ug8THa';") \
    .option("checkpointLocation", "HW06/checkpoints_2") \
    .start() \
    .awaitTermination()
