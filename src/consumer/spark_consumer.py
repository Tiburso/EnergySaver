from pyspark.sql import SparkSession

# Initialize Spark Session
spark = (
    SparkSession.builder.appName("PySparkKafkaConsumer")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2")
    .getOrCreate()
)

kafka_options = {
    "kafka.bootstrap.servers": "localhost:29092",
    "subscribe": "test",
}

# Read data from Kafka
df = spark.readStream.format("kafka").options(**kafka_options).load()

# Extract the value field and decode it as a UTF-8 string
df = df.selectExpr("CAST(value AS STRING)")

# Write the output to the console
query = df.writeStream.outputMode("append").format("console").start()

query.awaitTermination()
