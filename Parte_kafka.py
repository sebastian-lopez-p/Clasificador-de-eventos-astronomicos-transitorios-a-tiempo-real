from os import listdir
from kafka import KafkaProducer
from fastavro import reader
import gzip
import io
from astropy.io import fits
import struct
import time
"""
la forma de ejecutar esto es con la maquina virtual de mi pc (Thomas)
con 2 temrinales en wsl se deben de ejecutar estas 2 lineas de comandos en la carpeta de kafka

bin/zookeeper-server-start.sh config/zookeeper.properties

bin/kafka-server-start.sh config/server.properties


"""
files_path = ".\\datos\\"
onlyfiles = [files_path + f for f in listdir(files_path)]
#print(onlyfiles)
metadata = ["ra","dec","magpsf","sigmapsf","isdiffpos","diffmaglim","fwhm","sgscore1","sgscore2","sgscore3",
            "distpsnr1","distpsnr2","distpsnr3","classtar","ndethist","ncovhist","chinr","sharpnr","approx_nondet",
            "gal_lat","gal_lng","ecl_lat","ecl_lng"]

PRODUCER = KafkaProducer(bootstrap_servers="172.28.190.252", value_serializer=lambda v: v)

for file in onlyfiles:
    with open(file, 'rb') as f:
        avro_reader = reader(f)

        for record in avro_reader:
            for name in ["objectId", "candidate", "cutoutScience", "cutoutTemplate", "cutoutDifference"]:
                cutout = record[name]

                if name == "objectId":
                    #print(cutout) # send
                    PRODUCER.send("Id", cutout.encode('utf-8'))

                elif name == "candidate":
                    for key in metadata:
                        if key in ["gal_lat","gal_lng","ecl_lat","ecl_lng"]:
                            if key in record.keys():
                                #print(record[key]) #send
                                PRODUCER.send("Metadata", record[key].to_bytes())

                            else:
                                #print(0) #send Nan
                                cero = 0
                                PRODUCER.send("Metadata", cero.to_bytes())

                        elif key == "approx_nondet":
                            #print(cutout.get("ncovhist") - cutout["ndethist"]) #send
                            resta = cutout.get("ncovhist") - cutout["ndethist"]
                            PRODUCER.send("Metadata", struct.pack('d', resta))

                        else:
                            print(cutout[key]) #send
                            if type(cutout[key]) == type(0.1):
                                PRODUCER.send("Metadata", struct.pack('d', cutout[key]))
                                
                            elif type(cutout[key]) == type(" "):
                                PRODUCER.send("Metadata", cutout[key].encode('utf-8'))

                if 'stampData' in cutout:

                    compressed_data = cutout['stampData']
                    with gzip.GzipFile(fileobj=io.BytesIO(compressed_data)) as gz:
                        hdulist = fits.open(gz)
                        image_data = hdulist[0].data
                        PRODUCER.send("Imagen", image_data.tobytes())

                    print(name, file)
                else:
                    print(f"{name} no contiene datos de imagen.")
    time.sleep(8)        
