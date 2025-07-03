from os import listdir
from kafka import KafkaProducer
from fastavro import reader
import gzip
import io
from astropy.io import fits
import time
import numpy as np
import json

"""
la forma de ejecutar esto es con la maquina virtual de mi pc (Thomas)
con 2 temrinales en wsl se deben de ejecutar estas 2 lineas de comandos en la carpeta de kafka

bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties
"""

files_path = "./datos/"
onlyfiles = [files_path + f for f in listdir(files_path)]

metadata_keys = ["ra","dec","magpsf","sigmapsf","isdiffpos","diffmaglim","fwhm","sgscore1","sgscore2","sgscore3",
                "distpsnr1","distpsnr2","distpsnr3","classtar","ndethist","ncovhist","chinr","sharpnr","approx_nondet",
                "gal_lat","gal_lng","ecl_lat","ecl_lng"]

# Configuración del productor Kafka
PRODUCER = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: v if isinstance(v, bytes) else str(v).encode('utf-8')
)

def decode_stamp(stamp_data, crop_to_21x21=True):
    """Decodifica stamp data de AVRO usando el mismo método que tu código de referencia"""
    if stamp_data:
        try:
            decompressed = gzip.decompress(stamp_data)
            with fits.open(io.BytesIO(decompressed), ignore_missing_simple=True) as hdu:
                arr = hdu[0].data.astype(np.float32)
            if arr.shape != (63, 63):
                print(f"Tamaño inesperado: {arr.shape}, esperado (63,63)")
                return None
            if crop_to_21x21:
                arr = arr[21:42, 21:42]
            return np.nan_to_num(arr, nan=0.0)
        except Exception as e:
            print(f"Error decodificando stamp: {e}")
    return None

print(f"Iniciando procesamiento de {len(onlyfiles)} archivos...")

for file_idx, file in enumerate(onlyfiles):
    print(f"Procesando archivo {file_idx+1}/{len(onlyfiles)}: {file}")
    
    try:
        with open(file, 'rb') as f:
            avro_reader = reader(f)

            for alert in avro_reader:
                # Obtener object_id y datos principales
                object_id = alert.get('objectId', 'Unknown')
                candidate = alert.get('candidate', {})
                
                ID = object_id.encode('utf-8')
                
                print(f"→ Procesando {object_id}")

                # Obtener los stamps de las tres imágenes
                science_stamp = alert.get('cutoutScience', {}).get('stampData', None)
                reference_stamp = alert.get('cutoutTemplate', {}).get('stampData', None)
                difference_stamp = alert.get('cutoutDifference', {}).get('stampData', None)

                # Decodificar cada imagen
                sci = decode_stamp(science_stamp)
                ref = decode_stamp(reference_stamp)
                diff = decode_stamp(difference_stamp)

                # Solo procesar si todas las imágenes son válidas
                if sci is not None and ref is not None and diff is not None:
                    
                    # Crear diccionario de metadatos usando la misma lógica que tu código de referencia
                    metadata_dict = {
                        "ra": candidate.get("ra", -999),
                        "dec": candidate.get("dec", -999),
                        "magpsf": candidate.get("magpsf", -999),
                        "sigmapsf": candidate.get("sigmapsf", -999),
                        "isdiffpos": int(candidate.get("isdiffpos", 'f') == 't'),
                        "diffmaglim": candidate.get("diffmaglim", -999),
                        "fwhm": candidate.get("fwhm", -999),
                        "sgscore1": candidate.get("sgscore1", -999),
                        "sgscore2": candidate.get("sgscore2", -999),
                        "sgscore3": candidate.get("sgscore3", -999),
                        "distpsnr1": candidate.get("distpsnr1", -999),
                        "distpsnr2": candidate.get("distpsnr2", -999),
                        "distpsnr3": candidate.get("distpsnr3", -999),
                        "classtar": candidate.get("classtar", -999),
                        "ndethist": candidate.get("ndethist", -1),
                        "ncovhist": candidate.get("ncovhist", -1),
                        "chinr": candidate.get("chinr", -999),
                        "sharpnr": candidate.get("sharpnr", -999),
                        "gal_lat": alert.get("gal_lat", -999),
                        "gal_lng": alert.get("gal_lng", -999),
                        "ecl_lat": alert.get("ecl_lat", -999),
                        "ecl_lng": alert.get("ecl_lng", -999),
                        "approx_nondet": candidate.get("ncovhist", -1) - candidate.get("ndethist", -1),
                    }

                    # Mostrar algunos metadatos para verificación
                    print(f"  RA: {metadata_dict['ra']:.4f}, DEC: {metadata_dict['dec']:.4f}")
                    print(f"  Mag: {metadata_dict['magpsf']}, FWHM: {metadata_dict['fwhm']}")

                    # Enviar metadatos a Kafka
                    metadata_json = json.dumps(metadata_dict)
                    PRODUCER.send("Metadata", metadata_json, ID)
                    
                    # Apilar las imágenes en 3D (mismo orden que tu código: science, reference, difference)
                    stamps_3d = np.stack([sci, ref, diff])  # Shape: (3, 21, 21)
                    
                    # Reorganizar para que sea (21, 21, 3) como en tu código original
                    image_data = np.transpose(stamps_3d, (1, 2, 0))  # (21, 21, 3)
                    
                    print(f"  Forma de imagen: {image_data.shape}")
                    
                    # Convertir a float32 y enviar a Kafka
                    image_float32 = image_data.astype(np.float32)
                    PRODUCER.send("Imagen", image_float32.tobytes(), ID)
                    
                    print(f"Enviado correctamente")
                
                else:
                    print(f"Una o más imágenes no pudieron decodificarse")
                    if sci is None:
                        print("    - Science stamp inválido")
                    if ref is None:
                        print("    - Reference stamp inválido")
                    if diff is None:
                        print("    - Difference stamp inválido")

    except Exception as e:
        print(f"Error procesando archivo {file}: {e}")
        continue
    
    # Flush al final de cada archivo
    PRODUCER.flush()
    print(f"Archivo completado\n")
    time.sleep(1)

print("Procesamiento completado. Cerrando productor...")
PRODUCER.close()