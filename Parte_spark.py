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
"""

"""
def binario_a_array(bin_data):
    try:
        arr = np.frombuffer(bin_data, dtype=np.float32).reshape(21,21,3)
        return arr.flatten().tolist()
    except Exception:
        return [0.0] * (21 * 21 * 3)

udf_array = udf(binario_a_array, ArrayType(ArrayType(ArrayType(FloatType()))))

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
    StructField("chinr", DoubleType()),
    StructField("sharpnr", DoubleType()),
    StructField("approx_nondet", DoubleType()),
    StructField("gal_lat", DoubleType()),
    StructField("gal_lng", DoubleType()),
    StructField("ecl_lat", DoubleType()),
    StructField("ecl_lng", DoubleType())
    ])

spark = SparkSession.builder \
    .appName("Clasificacion_imagenes") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5") \
    .getOrCreate()

spark.sparkContext.addFile(".\\Modelo\\modelo_entrenado_v5.keras")
model_file_local = SparkFiles.get("modelo_entrenado_v5.keras")
modelo = tf.keras.models.load_model(model_file_local, compile=False)

def prediccion(arr):
    try:
        arr = np.array(arr)
        arr = np.reshape(arr, (21,21,3))
        arr = np.transpose(arr, (2,0,1))
        arr = np.expand_dims(arr, axis=0)  # (1, 21, 21, 3)
        pred = modelo.predict(arr, verbose=0)
        return int(np.argmax(pred))
    except Exception:
        return -1 

udf_prediccion = udf(prediccion, IntegerType())

df_imagen_bin = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "172.28.190.252:9092") \
    .option("subscribe", "Imagen") \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(key AS STRING) as key", "value") \
    .withColumn("imagen_array", udf_array("value"))

df_imagen_array = df_imagen_bin.withColumn("imagen_array", udf_array("value"))

df_metadata_bin = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "172.28.190.252:9092") \
    .option("subscribe", "Metadata") \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(key AS STRING) as key", "CAST(value AS STRING) as json") \
    .withColumn("metadata", from_json(col("json"), schema_metadata)) \
    .select("key", "metadata.*")

df_final = df_imagen_array.join(df_metadata_bin, on="key", how="inner") \
    .withColumnRenamed("key", "ID") \
    .withColumn("prediccion", udf_prediccion("imagen_array")) 

query = df_final.select("ID", "imagen_array", "prediccion", "ra", "dec", "magpsf", "sigmapsf", "isdiffpos", "diffmaglim", "fwhm", "sgscore1", "sgscore2", "sgscore3",  
                   "distpsnr1", "distpsnr2", "distpsnr3", "classtar", "ndethist", "ncovhist", "chinr", "sharpnr", "approx_nondet", "gal_lat", "gal_lng", "ecl_lat", "ecl_lng") \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
