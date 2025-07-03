from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *
import pandas as pd
import numpy as np
import json

# ---------- Esquema de metadatos CORREGIDO ----------
schema_metadata = StructType([
    StructField("ra", DoubleType()),
    StructField("dec", DoubleType()),
    StructField("magpsf", DoubleType()),
    StructField("sigmapsf", DoubleType()),
    StructField("isdiffpos", IntegerType()),  # CORREGIDO: era DoubleType
    StructField("diffmaglim", DoubleType()),
    StructField("fwhm", DoubleType()),
    StructField("sgscore1", DoubleType()),
    StructField("sgscore2", DoubleType()),
    StructField("sgscore3", DoubleType()),
    StructField("distpsnr1", DoubleType()),
    StructField("distpsnr2", DoubleType()),
    StructField("distpsnr3", DoubleType()),
    StructField("classtar", DoubleType()),
    StructField("ndethist", IntegerType()),  # CORREGIDO: era DoubleType
    StructField("ncovhist", IntegerType()),  # CORREGIDO: era DoubleType
    StructField("chinr", DoubleType()),
    StructField("sharpnr", DoubleType()),
    StructField("approx_nondet", IntegerType()),  # CORREGIDO: era DoubleType
    StructField("gal_lat", DoubleType()),
    StructField("gal_lng", DoubleType()),
    StructField("ecl_lat", DoubleType()),
    StructField("ecl_lng", DoubleType())
])

# ---------- Crear sesión Spark OPTIMIZADA ----------
spark = SparkSession.builder \
    .appName("Kafka_Spark_Optimizado") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.default.parallelism", "2") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.sql.streaming.metricsEnabled", "false") \
    .getOrCreate()

# Configurar nivel de log
spark.sparkContext.setLogLevel("ERROR")
log4j = spark._jvm.org.apache.log4j
log4j.LogManager.getRootLogger().setLevel(log4j.Level.ERROR)

print("[INFO] Sesión Spark iniciada con configuración optimizada")

# ---------- Stream de metadatos ----------
df_metadata = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "Metadata") \
    .option("startingOffsets", "latest") \
    .option("maxOffsetsPerTrigger", "5") \
    .option("fetchOffset.numRetries", "3") \
    .load() \
    .selectExpr("CAST(key AS STRING) as object_id", "CAST(value AS STRING) as json_data") \
    .withColumn("metadata", from_json(col("json_data"), schema_metadata)) \
    .select("object_id", "metadata.*")

# ---------- Stream de imágenes ----------
df_imagen = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "Imagen") \
    .option("startingOffsets", "latest") \
    .option("maxOffsetsPerTrigger", "5") \
    .option("fetchOffset.numRetries", "3") \
    .load() \
    .selectExpr("CAST(key AS STRING) as object_id", "value as image_data")

# Variable para evitar duplicados
processed_ids = set()

# ---------- Función de procesamiento SIMPLIFICADA ----------
def process_batch_optimized(df, epoch_id):
    global processed_ids
    
    print(f"\n[BATCH {epoch_id}] Iniciando procesamiento...")
    
    count = df.count()
    if count == 0:
        print(f"[BATCH {epoch_id}] Sin datos nuevos, esperando...")
        return
    
    print(f"[BATCH {epoch_id}] Procesando {count} registros")
    
    try:
        # Deduplicar en Spark antes de convertir a Pandas
        pdf = df.dropDuplicates(["object_id"]).toPandas()
        
        new_objects = 0
        for idx, row in pdf.iterrows():
            object_id = row['object_id']
            
            # Evitar duplicados entre batches
            if object_id in processed_ids:
                continue
            
            processed_ids.add(object_id)
            new_objects += 1
            
            print(f" [{new_objects}] Procesando ID: {object_id}")

            # Procesado de imagen
            try:
                image_bytes = row['image_data']
                if image_bytes is not None:
                    arr = np.frombuffer(image_bytes, dtype=np.float32)
                    
                    if arr.size == 1323:  # 21x21x3
                        image_shape = (21, 21, 3)
                        print(f"  Imagen: {image_shape}")
                        # Aquí agregamos el procesamiento extra de la imagen ------------------------
                        # ---------------------------------------------------------------------------
                    else:
                        print(f"  Tamaño inesperado: {arr.size} elementos")
                else:
                    print(f"  Imagen vacía")
                    
            except Exception as e:
                print(f"  Error procesando imagen: {e}")
            
            # Mostrar metadatos clave
            print(f"    RA: {row.get('ra', 'N/A'):.4f}, DEC: {row.get('dec', 'N/A'):.4f}")
            print(f"    Mag: {row.get('magpsf', 'N/A')}, FWHM: {row.get('fwhm', 'N/A')}")
            print(f"    ndethist: {row.get('ndethist', 'N/A')}, ncovhist: {row.get('ncovhist', 'N/A')}")
            
        print(f"[BATCH {epoch_id}] Objetos nuevos procesados: {new_objects}")
            
    except Exception as e:
        print(f"[BATCH {epoch_id}] Error: {e}")
    
    print(f"[BATCH {epoch_id}] Completado\n{'-'*50}")

# ---------- Join simple ----------
df_joined = df_imagen.join(df_metadata, on="object_id", how="inner")

# ---------- Query ----------
print("[INFO] Iniciando stream processing...")
print("[INFO] Presiona Ctrl+C para detener")

query = df_joined.writeStream \
    .foreachBatch(process_batch_optimized) \
    .outputMode("append") \
    .trigger(processingTime='10 seconds') \
    .option("checkpointLocation", "/tmp/checkpoint/kafka_stream") \
    .start()

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("\n[INFO] Deteniendo stream...")
    query.stop()
    spark.stop()
    print("[INFO] Stream detenido correctamente")
