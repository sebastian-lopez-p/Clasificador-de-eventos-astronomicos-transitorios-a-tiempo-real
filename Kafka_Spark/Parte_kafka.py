from os import listdir
from kafka import KafkaProducer
from fastavro import reader
import gzip
import io
from astropy.io import fits
import struct
import time
import numpy as np
"""
la forma de ejecutar esto es con la maquina virtual de mi pc (Thomas)
con 2 temrinales en wsl se deben de ejecutar estas 2 lineas de comandos en la carpeta de kafka

bin/zookeeper-server-start.sh config/zookeeper.properties

bin/kafka-server-start.sh config/server.properties
"""

files_path = ".\\datos\\"
onlyfiles = [files_path + f for f in listdir(files_path)]

metadata = ["ra","dec","magpsf","sigmapsf","isdiffpos","diffmaglim","fwhm","sgscore1","sgscore2","sgscore3",
            "distpsnr1","distpsnr2","distpsnr3","classtar","ndethist","ncovhist","chinr","sharpnr","approx_nondet",
            "gal_lat","gal_lng","ecl_lat","ecl_lng"]

PRODUCER = KafkaProducer(bootstrap_servers="172.28.190.252", value_serializer=lambda v: v)

for file in onlyfiles:
    with open(file, 'rb') as f:
        avro_reader = reader(f)

        for record in avro_reader:
            ID = record["objectId"].encode('utf-8')
            for name in ["candidate", "cutoutScience", "cutoutTemplate", "cutoutDifference"]:
                cutout = record[name]

                print(record["objectId"],"------------------------------------------------------------------")

                if name == "candidate":
                    for key in metadata:
                        if key in ["gal_lat","gal_lng","ecl_lat","ecl_lng"]:
                            if key in record.keys():
                                print(record[key], f"{key}") #send
                                PRODUCER.send("Metadata", record[key].to_bytes(), ID)

                            else:
                                print(0, f"{key}") #send Nan
                                cero = 0
                                PRODUCER.send("Metadata", cero.to_bytes(), ID)

                        elif key == "approx_nondet":
                            print(cutout.get("ncovhist") - cutout["ndethist"], f"{key}") #send
                            resta = cutout.get("ncovhist") - cutout["ndethist"]
                            PRODUCER.send("Metadata", struct.pack('d', resta), ID)

                        else:
                            print(cutout[key], f"{key}") #send
                            if type(cutout[key]) == type(0.1):
                                PRODUCER.send("Metadata", struct.pack('d', cutout[key]), ID)

                            elif type(cutout[key]) == type(" "):
                                PRODUCER.send("Metadata", cutout[key].encode('utf-8'), ID)

                if 'stampData' in cutout:
                    compressed_data = cutout['stampData']
                    if name == "cutoutScience":
                        with gzip.GzipFile(fileobj=io.BytesIO(compressed_data)) as gz:
                            hdulist = fits.open(gz)
                            image_data_s = hdulist[0].data

                    elif name == "cutoutTemplate":
                        with gzip.GzipFile(fileobj=io.BytesIO(compressed_data)) as gz:
                            hdulist = fits.open(gz)
                            image_data_t = hdulist[0].data

                    elif name == "cutoutDifference":
                        with gzip.GzipFile(fileobj=io.BytesIO(compressed_data)) as gz:
                            hdulist = fits.open(gz)
                            image_data_d = hdulist[0].data

            image_data = np.dstack((image_data_s, image_data_t, image_data_d))[21:42, 21:42, :]
            print(image_data.shape, "imagenes")
            PRODUCER.send("Imagen", image_data.flatten().tobytes(), ID)
                    
    PRODUCER.flush()
    time.sleep(1)
