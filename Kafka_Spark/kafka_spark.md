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
El codigo llamado kafka_v3.py lee los archivos .avro con alertas astronómicas (de ZTF u otros surveys), lo que hace es extraer los metadatos que no serán útiles para pasarlos a la red neuronal junto a las imagenes (al igual que lo hace la parte del procesamiento de los datos), para luego enviar esta información a kafka para ser procesada o distribuida en tiempo real

### resumen del flujo del codigo
  1. Busca los archivos .avro dentro de una carpeta
  2. abre cada archivo y lee las alertas una por una
  3. para cada alerta:
     - extrae los metadatos junto al ID del objeto
     - extrae las tres imagenes que nos serán utiles para la capa convulucional (ciencia, diferencia y referencia)
  4. se repite para todos los archivos
 

# Spark
## Funcionamiento del código
Esta parte consiste en la implementacion de la red neuronal para obtener el csv con los objetos astronomicos ya clasificados:
### Flujo de codigo
  1. recibe los datos desde kafka con dos topicos (metadatos e imagenes)
  2. une los datos de metadatos e imagenes usando la id
  3. limpia los datos
  4. clasifica cada objeto con un modelo (previamente entrenado)
     - aclaración: muchas veces tuvimos errores por la forma en la que subiamos la red neuronal, por lo tanto se aplica un codigo en donde se generan predicciones aleatorias para corroborar que el codigo andubiera bien
  6. guard el resultado en un archivo CSV con:
     - ID del objeto, clase, confianza
     - todos los metadatos asociados
     - indicadores de si los datos fueron correctamente procesados
  7. repite.

# Resultado final:
El resultado final de este proyecto será un csv con metadatos (incluyendo el ID) e imagenes con su clasificacion 

