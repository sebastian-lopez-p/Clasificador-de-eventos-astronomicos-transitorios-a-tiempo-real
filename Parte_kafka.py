from os import listdir
from kafka import KafkaProducer
from fastavro import reader
import gzip
import io
from astropy.io import fits

files_path = ".\\datos\\"
onlyfiles = [files_path + f for f in listdir(files_path)]
#print(onlyfiles)


# Kafka topic
PRODUCER = KafkaProducer(bootstrap_servers="172.28.190.252", value_serializer=lambda v: v)
TOPIC = "Imagen"

for file in onlyfiles:
    with open(file, 'rb') as f:
        avro_reader = reader(f)
        for record in avro_reader:
            # Accede a los cutouts
            for name in ["cutoutScience", "cutoutTemplate", "cutoutDifference"]:
                cutout = record[name]

                if 'stampData' in cutout:
                    # Extraer y descomprimir los datos gzip
                    compressed_data = cutout['stampData']
                    with gzip.GzipFile(fileobj=io.BytesIO(compressed_data)) as gz:
                        hdulist = fits.open(gz)
                        image_data = hdulist[0].data
                        PRODUCER.send(TOPIC, image_data.tobytes())
                        # Mostrar la imagen
                    print(name, file)
                else:
                    print(f"{name} no contiene datos de imagen.")
            
            break  # Solo el primer registro

