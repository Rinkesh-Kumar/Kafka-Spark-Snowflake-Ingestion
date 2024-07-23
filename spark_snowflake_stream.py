from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,TimestampType
import json

#foreachBatch function
def write_to_snowflake(batch_df,batch_id):
    #Snowflake credentials
    with open("cred.json","r") as f:
        sfOptions = json.load(f)

    SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
    batch_df.write\
        .format(SNOWFLAKE_SOURCE_NAME)\
        .options(**sfOptions)\
        .option("dbtable", "TELECOM_DATA") \
        .mode("append") \
        .save()

# Initialize spark session
spark = SparkSession.builder \
    .appName("SnowflakeKafkaDataWrite") \
    .config("spark.jars", "spark-snowflake_2.12-2.12.0-spark_3.3.jar,snowflake-jdbc-3.13.30.jar")  \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2")\
    .config("spark.sql.shuffle.partitions","2")\
    .getOrCreate()

#Kafka credentials
with open("kafka_config.json","r") as f:
    kafka_config = json.load(f)

kafkaOptions = {
    "kafka.sasl.jaas.config": f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_config["sasl.username"]}" password="{kafka_config["sasl.password"]}";',
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.security.protocol" : "SASL_SSL",
    "kafka.bootstrap.servers": kafka_config['bootstrap.servers'],
    "group.id": "group-01",
    "subscribe": "telecom_data",
}

#Schema of Incoming data
schema = StructType() \
    .add("caller_name", StringType()) \
    .add("receiver_name", StringType()) \
    .add("caller_id", StringType()) \
    .add("receiver_id", StringType()) \
    .add("start_datetime", TimestampType()) \
    .add("end_datetime", TimestampType()) \
    .add("call_duration", IntegerType()) \
    .add("network_provider", StringType()) \
    .add("total_amount", StringType())

#Read stream data
spark.sparkContext.setLogLevel("ERROR")

df = spark.readStream.format("kafka")\
    .options(**kafkaOptions)\
    .option("startingOffset","latest")\
    .load()

df = df.selectExpr("CAST(value AS STRING)")\
    .select(from_json(col("value"),schema).alias("data"))\
    .select("data.*")

df.printSchema()


print("Streaming Strated !!")
print("****************************************")

query = df.writeStream\
        .foreachBatch(write_to_snowflake)\
        .outputMode("update")\
        .start()
query.awaitTermination()