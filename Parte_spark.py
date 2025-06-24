from pyspark import SparkFiles
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import from_json, col , expr, udf
from pyspark.sql.types import *
import numpy as np
import tensorflow as tf

import os
os.environ["PYSPARK_PYTHON"] = "python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python"


spark = SparkSession.builder \
    .appName("Clasificacion_imagenes") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5") \
    .getOrCreate()

spark.sparkContext.addFile(".\\Modelo\\modelo_entrenado_v5.keras")
model_file_local = SparkFiles.get("modelo_entrenado_v5.keras")
modelo = tf.keras.models.load_model(model_file_local, compile=False)

def predecir_desde_binario(bin_data):
    try:
        arr = np.frombuffer(bin_data, dtype=np.float32).reshape((3, 21, 21))
        arr = np.moveaxis(arr, 0, -1)  # (21, 21, 3)
        arr = np.expand_dims(arr, axis=0)  # (1, 21, 21, 3)
        pred = modelo.predict(arr, verbose=0)
        return int(np.argmax(pred))
    except Exception as e:
        return -1 

predict_udf = udf(predecir_desde_binario, IntegerType())

schema_metadata = StructType([
    StructField("ra", DoubleType()),
    StructField("dec", DoubleType()),
    StructField("magpsf", DoubleType()),
    StructField("sigmapsf", DoubleType()),
    StructField("isdiffpos", DoubleType()),
    StructField("diffmaglim", DoubleType()),
    StructField("fwhm", DoubleType()),
    StructField("sgscore1", DoubleType()),
    StructField("sgscore2", DoubleType()),
    StructField("sgscore3", DoubleType()),
    StructField("distpsnr1", DoubleType()),
    StructField("distpsnr2", DoubleType()),
    StructField("distpsnr3", DoubleType()),
    StructField("classtar", DoubleType()),
    StructField("ndethist", DoubleType()),
    StructField("ncovhist", DoubleType()),
    StructField("chinr", StringType()),
    StructField("sharpnr", StringType()),
    StructField("approx_nondet", DoubleType()),
    StructField("gal_lat", DoubleType()),
    StructField("gal_lng", DoubleType()),
    StructField("ecl_lat", DoubleType()),
    StructField("ecl_lng", DoubleType())
    ])

df_imagen = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "172.28.190.252:9092") \
    .option("subscribe", "Imagen") \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(key AS STRING) as key", "value") \
    .withColumn("prediccion", predict_udf(col("value")))

df_metadata = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "172.28.190.252:9092") \
    .option("subscribe", "Metadata") \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(key AS STRING) as key", "CAST(value AS STRING) as json") \
    .withColumn("metadata", from_json(col("json"), schema_metadata)) \
    .select("key", "metadata.*")

#spark.sparkContext.broadcast(predict())

df_final = df_imagen.join(df_metadata, on="key", how="inner")

query = df_final.select("key", "prediccion") \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
