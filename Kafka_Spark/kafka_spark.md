# Lectura y clasificación en tiempo real 
## Antes de todo 
En dos terminales diferentes de ubuntu con kafka instalado y en las configuraciones de kafka haber puesto la IP del dispositivo, ejecutar los siguientes batches (deben ejecutarse dentro de la carpeta de kafka):
- ``bin/zookeeper-server-start.sh config/zookeeper.properties``
- ``bin/kafka-server-start.sh config/server.properties``

Esto hará que kafka se inicie con un servidor. 

El codigo debe ejecutarse junto a la carpeta que tendrá los datos 

``PRODUCER = KafkaProducer(bootstrap_servers="IP_pc", value_serializer=lambda v: v)``
En esta parte donde dice IP_pc asegurarse.....*

También hay que tener en consideración que los datos sean en formato .avro

# Kafka
## funcionamiento del código
El codigo recorrerá toda la carpeta de los datos, dentro de un ciclo recorrerá cada diccionario asegurandose de que los valores contengan la metadata que se necesita para la clasificación, para luego mandarla a través de kafka en formato binario (.npy), luego se asegura que también exista "stampData", la cual es la "subcarpeta" donde se tienen la informacion de las imagenes, siendo enviada por kafka en formato binario. Toda la información lleva la misma ID de la imagen que contenia el archivo de los datos (key)

# Spark
## Funcionamiento del código
