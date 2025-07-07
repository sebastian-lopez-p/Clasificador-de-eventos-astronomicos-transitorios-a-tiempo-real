# Clasificación de eventos astronomicos transitorios a tiempo real 
## Integrantes
- Andrés Rumillanca
- Sebastián López
- Thomas Salazar
- Ignacio Solís

## La problematica 
Un proyecto interesante en la astronomia es la clasificación de cuerpos astronomicos a través de redes neuronales. Nuestro proyecto consiste en clasificar eventos detectados por ZTF (vease sitio web...) los cuales consisten en eventos que son "poco" cumunes y alteran o fluctuan el mapa del cielo nocturno, estos eventos pueden ser objetos tales como; meteoritos, AGN, supernovas, estrellas variables, etc.

## El plan
Nuestro proyecto se divide en 3 pasos:
- Manejo de datos
- Modelo
- Kafka/spark

# Manejo de datos
El manejo de datos es la primera parte del pryecto, pues se encargará de limpiar, optimizar y filtrar los datos para que se encuentren en un formato mas amigable para la red neuronal, cabe recalcar que los datos son descargados en formato .avro (ver archivo "Manejo_de_datos.ipynb" para tener más información).

# Modelo
El modelo es lo escencial y la base del proyecto, pues es la red neuronal quien se encargará de clasificar futuras imagenes y datos en tiempo real, en este apartado nos encargamos de entrenar la red neuronal y revisar cautelosamente que tan buena es dicha red para la clasificación (ver archivo "ClairObscur_Modelo_33" para más información).

# Kafka/Spark
Este apartado sería lo tangible de nuestro proyecto, pues es la parte final, donde se enviarán los datos en tiempo real mientras estos serán clasificados por la red neuronal (vease los siguientes 3 archivos para mayor entendimiento: "Parte_kafka.py", "Parte_spark.py", "kafka_spark.md")
