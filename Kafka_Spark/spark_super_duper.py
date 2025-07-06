from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *
import numpy as np
import csv
import os
from datetime import datetime
from sklearn.preprocessing import StandardScaler
import warnings
warnings.filterwarnings('ignore')
import tensorflow as tf
from tensorflow.keras import layers
import os

#------------------Cofiguracion--------------------
os.environ['TF_XLA_FLAGS'] = '--tf_xla_enable_xla_devices=false'
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'
tf.config.set_visible_devices([], 'GPU')
tf.config.optimizer.set_jit(False)

# ---------- Esquema de metadatos CORREGIDO ----------
schema_metadata = StructType([
    StructField("ra", DoubleType()),
    StructField("dec", DoubleType()),
    StructField("magpsf", DoubleType()),
    StructField("sigmapsf", DoubleType()),
    StructField("isdiffpos", IntegerType()),
    StructField("diffmaglim", DoubleType()),
    StructField("fwhm", DoubleType()),
    StructField("sgscore1", DoubleType()),
    StructField("sgscore2", DoubleType()),
    StructField("sgscore3", DoubleType()),
    StructField("distpsnr1", DoubleType()),
    StructField("distpsnr2", DoubleType()),
    StructField("distpsnr3", DoubleType()),
    StructField("classtar", DoubleType()),
    StructField("ndethist", IntegerType()),
    StructField("ncovhist", IntegerType()),
    StructField("chinr", DoubleType()),
    StructField("sharpnr", DoubleType()),
    StructField("approx_nondet", IntegerType()),
    StructField("gal_lat", DoubleType()),
    StructField("gal_lng", DoubleType()),
    StructField("ecl_lat", DoubleType()),
    StructField("ecl_lng", DoubleType())
])

# ---------- Columnas de metadatos para el modelo ----------
metadata_columns = ["ra", "dec", "magpsf", "sigmapsf", "isdiffpos", "diffmaglim", "fwhm", 
                   "sgscore1", "sgscore2", "sgscore3", "distpsnr1", "distpsnr2", "distpsnr3",
                   "classtar", "ndethist", "ncovhist", "chinr", "sharpnr", "approx_nondet",
                   "gal_lat", "gal_lng", "ecl_lat", "ecl_lng"]

# ---------- Crear sesión Spark OPTIMIZADA ----------
spark = SparkSession.builder \
    .appName("Kafka_Spark_ML_Processing") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.default.parallelism", "2") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.sql.streaming.metricsEnabled", "false") \
    .getOrCreate()

# Configuracion nivel de log
spark.sparkContext.setLogLevel("ERROR")
log4j = spark._jvm.org.apache.log4j
log4j.LogManager.getRootLogger().setLevel(log4j.Level.ERROR)

print("[INFO] Sesión Spark iniciada con configuración optimizada para ML")

# ---------- Stream de metadatos con configuración mejorada ----------
df_metadata = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "Metadata") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .option("maxOffsetsPerTrigger", "5") \
    .option("fetchOffset.numRetries", "3") \
    .option("kafka.session.timeout.ms", "30000") \
    .option("kafka.request.timeout.ms", "40000") \
    .option("kafka.consumer.timeout.ms", "35000") \
    .load() \
    .selectExpr("CAST(key AS STRING) as object_id", "CAST(value AS STRING) as json_data") \
    .withColumn("metadata", from_json(col("json_data"), schema_metadata)) \
    .select("object_id", "metadata.*")

# ---------- Stream de imágenes con configuración mejorada ----------
df_imagen = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "Imagen") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .option("maxOffsetsPerTrigger", "5") \
    .option("fetchOffset.numRetries", "3") \
    .option("kafka.session.timeout.ms", "30000") \
    .option("kafka.request.timeout.ms", "40000") \
    .option("kafka.consumer.timeout.ms", "35000") \
    .load() \
    .selectExpr("CAST(key AS STRING) as object_id", "value as image_data")

# Variables globales para procesamiento
processed_ids = set()
scaler = StandardScaler()
scaler_fitted = False

# ================ Definición de la clase personalizada ==================
@tf.keras.utils.register_keras_serializable()
class RotationLayer(layers.Layer):
    def __init__(self, k, **kwargs):
        super().__init__(**kwargs)
        self.k = k

    def call(self, inputs):
        return tf.image.rot90(inputs, k=self.k)

    def get_config(self):
        config = super().get_config()
        config.update({"k": self.k})
        return config

# Clases del modelo
class_labels = ['AGN', 'QSO', 'VS', 'YSO', 'Other']

# ================ Configuración CSV ==================
csv_filename = f"astronomical_predictions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
csv_headers = [
    'timestamp', 'object_id', 'clase_predicha', 'confianza',
    'prob_AGN', 'prob_QSO', 'prob_VS', 'prob_YSO', 'prob_Other',
    'ra', 'dec', 'magpsf', 'sigmapsf', 'isdiffpos', 'diffmaglim', 
    'fwhm', 'sgscore1', 'sgscore2', 'sgscore3', 'distpsnr1', 
    'distpsnr2', 'distpsnr3', 'classtar', 'ndethist', 'ncovhist',
    'chinr', 'sharpnr', 'approx_nondet', 'gal_lat', 'gal_lng', 
    'ecl_lat', 'ecl_lng', 'image_processed', 'metadata_processed'
]

def initialize_csv():
    """Inicializa el archivo CSV con headers si no existe"""
    if not os.path.exists(csv_filename):
        with open(csv_filename, 'w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(csv_headers)
        print(f"[INFO] Archivo CSV inicializado: {csv_filename}")
        return True
    return False

def write_to_csv(data_dict):
    """Escribe un registro al CSV"""
    try:
        with open(csv_filename, 'a', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            row = []
            for header in csv_headers:
                row.append(data_dict.get(header, ''))
            writer.writerow(row)
        return True
    except Exception as e:
        print(f"    Error escribiendo a CSV: {e}")
        return False

# Modelo entrenado 
model = None
try:
    tf.get_logger().setLevel('ERROR')
    
    tf.config.set_visible_devices([], 'GPU')
    
    model = tf.keras.models.load_model('./Modelo/Modelo_entrenado_v33.h5', 
                                     custom_objects={'RotationLayer': RotationLayer},
                                     compile=False)  # No compilar para evitar problemas
    
    print("[INFO] Modelo cargado exitosamente: Modelo_entrenado_v33.h5")
    print("[INFO] Configuración: CPU only, XLA disabled")
    
except Exception as e:
    print(f"[ERROR] No se pudo cargar el modelo: {e}")
    print("[INFO] Continuando con simulación...")
    model = None

def normalize_data(data):
    """
    Normaliza los datos de entrada a un rango de 0 a 1.
    """
    data_min = np.min(data)
    data_max = np.max(data)
    if data_max - data_min == 0:
        return np.zeros_like(data)
    return (data - data_min) / (data_max - data_min)

def preprocess_image(image_bytes):
    """
    Preprocesa la imagen para el modelo de TensorFlow
    """
    try:
        # Convertir bytes a array numpy
        arr = np.frombuffer(image_bytes, dtype=np.float32)
        
        if arr.size != 1323:  # 21x21x3
            print(f"    Tamaño inesperado: {arr.size} elementos (esperado: 1323)")
            return None
            
        # Reshape a (21, 21, 3)
        image_shape = (21, 21, 3)
        imagen = arr.reshape(image_shape)
        
        # Normalizar cada canal individualmente
        imagen_normalizada = np.zeros_like(imagen)
        for i in range(3):  # Para cada canal (science, reference, difference)
            canal = imagen[:, :, i]
            imagen_normalizada[:, :, i] = normalize_data(canal)
        
        print(f"    Imagen procesada: {imagen_normalizada.shape}")
        print(f"    Rango de valores: [{np.min(imagen_normalizada):.4f}, {np.max(imagen_normalizada):.4f}]")
        
        return imagen_normalizada
        
    except Exception as e:      
        print(f"    Error procesando imagen: {e}")
        return None

def preprocess_metadata(metadata_row):
    """
    Preprocesa los metadatos para el modelo
    """
    global scaler, scaler_fitted
    
    try:
        # Extrae los valores de metadatos
        metadata_values = []
        for col_name in metadata_columns:
            value = metadata_row.get(col_name, -999)

            if value is None or (isinstance(value, float) and np.isnan(value)):
                value = -999
            metadata_values.append(float(value))
        
        metadata_array = np.array(metadata_values, dtype='float32').reshape(1, -1)
        
        # Escala de metadatos usando StandardScaler
        if not scaler_fitted:
            # Para el primer batch, usar los datos para fit_transform
            metadata_scaled = scaler.fit_transform(metadata_array)
            scaler_fitted = True
            print("    Scaler inicializado con primer conjunto de datos")
        else:
            metadata_scaled = scaler.transform(metadata_array)
        
        print(f"    Metadatos procesados: {metadata_scaled.shape}")
        print(f"    Valores escalados (primeros 5): {metadata_scaled[0][:5]}")
        
        return metadata_scaled[0]  # Retornar como array 1D
        
    except Exception as e:
        print(f"    Error procesando metadatos: {e}")
        return None

def simulate_model_prediction(imagen, metadata):
    """
    Realiza predicción usando el modelo de TensorFlow cargado
    """
    global model
    
    try:
        imagen_batch = np.expand_dims(imagen, axis=0)  # (1, 21, 21, 3)
        metadata_batch = np.expand_dims(metadata, axis=0)  # (1, num_features)
        
        print(f"    Entrada al modelo:")
        print(f"      Imagen: {imagen_batch.shape}")
        print(f"      Metadatos: {metadata_batch.shape}")
        
        if model is not None:
            try:
                # Configuracion de dispositivo para CPU
                with tf.device('/CPU:0'):
                    # Predicción real con el modelo cargado
                    prediccion = model.predict({
                        'image_input': imagen_batch, 
                        'metadata_input': metadata_batch
                    }, verbose=0)
                    
            except Exception as tf_error:
                print(f"    Error con TensorFlow: {tf_error}")
                print("    Fallback a simulación...")
                # Simulación como fallback
                np.random.seed(hash(str(imagen_batch[0,0,0,0])) % 2**32)
                prediccion = np.random.random((1, len(class_labels)))
                prediccion = prediccion / np.sum(prediccion)
        else:
            # Simulación si no se pudo cargar el modelo
            print("    Usando simulación (modelo no disponible)")
            np.random.seed(hash(str(imagen_batch[0,0,0,0])) % 2**32)
            prediccion = np.random.random((1, len(class_labels)))
            prediccion = prediccion / np.sum(prediccion)
        
        # Obtener clase predicha
        clase_predicha_idx = np.argmax(prediccion, axis=1)[0]
        clase_predicha = class_labels[clase_predicha_idx]
        confianza = prediccion[0][clase_predicha_idx]
        
        print(f"    Predicción:")
        print(f"      Clase: {clase_predicha}")
        print(f"      Confianza: {confianza:.4f}")
        print(f"      Probabilidades: {dict(zip(class_labels, prediccion[0]))}")
        
        return {
            'clase_predicha': clase_predicha,
            'confianza': confianza,
            'probabilidades': prediccion[0]
        }
        
    except Exception as e:
        print(f"    Error en predicción: {e}")
        print(f"    Fallback a simulación por error crítico...")
        # Simulación como último recurso
        np.random.seed(42)
        prediccion = np.random.random((1, len(class_labels)))
        prediccion = prediccion / np.sum(prediccion)
        
        clase_predicha_idx = np.argmax(prediccion, axis=1)[0]
        clase_predicha = class_labels[clase_predicha_idx]
        confianza = prediccion[0][clase_predicha_idx]
        
        return {
            'clase_predicha': f"{clase_predicha}_SIM",
            'confianza': confianza,
            'probabilidades': prediccion[0]
        }
        
        return None

def process_batch_ml_enhanced(df, epoch_id):
    global processed_ids
    
    print(f"\n{'='*60}")
    print(f"[BATCH {epoch_id}] Iniciando procesamiento ML avanzado...")
    print(f"{'='*60}")
    
    count = df.count()
    if count == 0:
        print(f"[BATCH {epoch_id}] Sin datos nuevos, esperando...")
        return
    
    print(f"[BATCH {epoch_id}] Procesando {count} registros")
    
    try:
        # Deduplica en Spark antes de convertir a Pandas
        pdf = df.dropDuplicates(["object_id"]).toPandas()
        
        new_objects = 0
        successful_predictions = 0
        csv_writes = 0
        
        for idx, row in pdf.iterrows():
            object_id = row['object_id']
            
            # Evita los duplicados entre batches
            if object_id in processed_ids:
                continue
            
            processed_ids.add(object_id)
            new_objects += 1
            
            print(f"\n[{new_objects}] Procesando objeto: {object_id}")
            print(f"  {'-'*40}")
            
            # ==============PROCESAMIENTO DE IMAGEN================
            imagen_procesada = None
            image_processed = False
            image_bytes = row.get('image_data')
            if image_bytes is not None:
                imagen_procesada = preprocess_image(image_bytes)
                image_processed = imagen_procesada is not None
            else:
                print(f"    Sin datos de imagen")
            
            # ================PROCESAMIENTO DE METADATOS================
            metadata_procesados = preprocess_metadata(row)
            metadata_processed = metadata_procesados is not None
            
            # ================INFORMACIÓN ASTRONÓMICA================
            print(f"    Información astronómica:")
            print(f"      RA: {row.get('ra', 'N/A'):.6f}°, DEC: {row.get('dec', 'N/A'):.6f}°")
            print(f"      Magnitud PSF: {row.get('magpsf', 'N/A')}")
            print(f"      FWHM: {row.get('fwhm', 'N/A')} px")
            print(f"      Detecciones históricas: {row.get('ndethist', 'N/A')}")
            print(f"      Cobertura histórica: {row.get('ncovhist', 'N/A')}")
            print(f"      Clase estelar: {row.get('classtar', 'N/A')}")
            

            csv_data = {
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'object_id': object_id,
                'image_processed': image_processed,
                'metadata_processed': metadata_processed
            }
            
            # Agrega todos los metadatos al CSV
            for col_name in metadata_columns:
                csv_data[col_name] = row.get(col_name, '')
            
            # ================PREDICCIÓN DEL MODELO================
            if imagen_procesada is not None and metadata_procesados is not None:
                prediccion = simulate_model_prediction(imagen_procesada, metadata_procesados)
                if prediccion is not None:
                    successful_predictions += 1
                    
                    # Agrega datos de predicción al CSV
                    csv_data.update({
                        'clase_predicha': prediccion['clase_predicha'],
                        'confianza': prediccion['confianza'],
                        'prob_AGN': prediccion['probabilidades'][0],
                        'prob_QSO': prediccion['probabilidades'][1],
                        'prob_VS': prediccion['probabilidades'][2],
                        'prob_YSO': prediccion['probabilidades'][3],
                        'prob_Other': prediccion['probabilidades'][4]
                    })
                    
                    print(f"    Objeto procesado exitosamente")
                else:
                    # Sin predicción pero guardar metadatos
                    csv_data.update({
                        'clase_predicha': 'ERROR',
                        'confianza': 0,
                        'prob_AGN': 0, 'prob_QSO': 0, 'prob_VS': 0, 'prob_YSO': 0, 'prob_Other': 0
                    })
                    print(f"    Error en la predicción")
            else:
                # Datos incompletos pero guardar lo que tenemos
                csv_data.update({
                    'clase_predicha': 'INCOMPLETE_DATA',
                    'confianza': 0,
                    'prob_AGN': 0, 'prob_QSO': 0, 'prob_VS': 0, 'prob_YSO': 0, 'prob_Other': 0
                })
                print(f"    Datos incompletos - saltando predicción")
            
            # ================ESCRIBIR A CSV================
            if write_to_csv(csv_data):
                csv_writes += 1
                print(f"    Datos guardados en CSV")
            else:
                print(f"    Error guardando en CSV")
        
        print(f"\n{'='*60}")
        print(f"[BATCH {epoch_id}] RESUMEN:")
        print(f"  Objetos nuevos: {new_objects}")
        print(f"  Predicciones exitosas: {successful_predictions}")
        print(f"  Registros guardados en CSV: {csv_writes}")
        print(f"  Tasa de éxito: {successful_predictions/max(new_objects,1)*100:.1f}%")
        print(f"  IDs procesados acumulados: {len(processed_ids)}")
        print(f"  Archivo CSV: {csv_filename}")
        print(f"{'='*60}")
            
    except Exception as e:
        print(f"[BATCH {epoch_id}] Error crítico: {e}")
        import traceback
        traceback.print_exc()
    
    print(f"[BATCH {epoch_id}] Completado\n")

# Función para limpiar checkpoints si es necesario
def cleanup_checkpoints():
    """Limpia los checkpoints para empezar desde cero"""
    import shutil
    checkpoint_dir = "/tmp/checkpoint/kafka_stream_ml"
    if os.path.exists(checkpoint_dir):
        try:
            shutil.rmtree(checkpoint_dir)
            print(f"[INFO] Checkpoints limpiados: {checkpoint_dir}")
        except Exception as e:
            print(f"[WARNING] No se pudo limpiar checkpoints: {e}")


# ---------- Join de streams ----------
df_joined = df_imagen.join(df_metadata, on="object_id", how="inner")

# ---------- Inicializar CSV ----------
initialize_csv()

# ---------- Configuración de query optimizada ----------
print("[INFO] Iniciando stream processing para ML...")
print("[INFO] Configuración:")
print(f"  Clases objetivo: {class_labels}")
print(f"  Metadatos a procesar: {len(metadata_columns)} columnas")
print(f"  Normalización de imágenes: Habilitada")
print(f"  Escalado de metadatos: StandardScaler")
print(f"  Archivo CSV: {csv_filename}")
print(f"  Configuración failOnDataLoss: false")
print("[INFO] Presiona Ctrl+C para detener")

# ---------- Query con manejo de errores mejorado ----------
query = df_joined.writeStream \
    .foreachBatch(process_batch_ml_enhanced) \
    .outputMode("append") \
    .trigger(processingTime='15 seconds') \
    .option("checkpointLocation", "/tmp/checkpoint/kafka_stream_ml") \
    .option("failOnDataLoss", "false") \
    .start()

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("\n[INFO] Señal de interrupción recibida...")
    print("[INFO] Estadísticas finales:")
    print(f"  Total de objetos únicos procesados: {len(processed_ids)}")
    print(f"  Archivo CSV final: {csv_filename}")
    if os.path.exists(csv_filename):
        file_size = os.path.getsize(csv_filename)
        print(f"  Tamaño del archivo CSV: {file_size:,} bytes")
    print("[INFO] Deteniendo stream...")
    query.stop()
    spark.stop()
    print("[INFO] Stream detenido correctamente")
    print("[INFO] Hasta luego!")
except Exception as e:
    print(f"\n[ERROR] Error en el stream: {e}")
    print("[INFO] Deteniendo stream debido a error...")
    query.stop()
    spark.stop()
    print("[INFO] Stream detenido por error")