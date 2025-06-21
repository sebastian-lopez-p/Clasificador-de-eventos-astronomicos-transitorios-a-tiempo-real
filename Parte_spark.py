from pyspark import SparkFiles
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, IntegerType

spark = SparkSession.builder \
    .appName("Clasificacion_imagenes") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5") \
    .getOrCreate()

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "172.28.190.252:9092") \
    .option("subscribe", "Imagen") \
    .option("startingOffsets", "earliest") \
    .load()

query = df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()