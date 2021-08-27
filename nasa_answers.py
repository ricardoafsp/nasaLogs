#!/usr/bin/env python
# coding: utf-8

# Comenzamos importando las librerias necesarias para poder trabajar
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import * 
import sys 

# Creamos una sesión usando SparkSession, incluyendo "enableHiveSupport" para poder conectarnos a hive y crear tablas en el.
spark = (SparkSession
.builder
.appName("Nasa")
.enableHiveSupport()
.getOrCreate())

# Declaramos la variable julio, indicando que será el archivo especificado en la ruta escrita.
#julio = "/nasaLogs/NASA_Access_Log_Jul95.gz"
julio = sys.argv[1]

# Leemos los datos obtenidos de la variable julio, indicando que el delimitador es " ".
nasaLogsJulio = spark.read.options(delimiter = " ").csv(julio)

# Mediante funciones regex hacemos la limpieza necesaria a los datos de manera de obtener mayor legibilidad de los mismos.
nasaLogsJulio = nasaLogsJulio.withColumn("_c3", F.regexp_replace(("_c3"), "\[", ""))
nasaLogsJulio = nasaLogsJulio.withColumn("_c4", F.regexp_replace(("_c4"), "\]", ""))
nasaLogsJulio = nasaLogsJulio.withColumn("RequestMethod", F.regexp_replace(("_c5"), "(\w+).*", "$1"))
nasaLogsJulio = nasaLogsJulio.withColumn("Resource", F.regexp_replace(("_c5"), "\S+\s(\S+).*", "$1"))
nasaLogsJulio = nasaLogsJulio.withColumn("Protocol", F.regexp_replace(("_c5"), "\S+\s+\S+\s(\S+).*", "$1"))
nasaLogsJulio = nasaLogsJulio.drop("_c5")

# Renombramos las columnas obtenidas de la limpieza antes realizada.
nasaLogsJulio = nasaLogsJulio.withColumnRenamed("_c0", "Host") \
                             .withColumnRenamed("_c1", "UserIdentifier") \
                             .withColumnRenamed("_c2", "UserId") \
                             .withColumnRenamed("_c4", "TimeZone") \
                             .withColumnRenamed("_c6", "HttpStatus") \
                             .withColumnRenamed("_c7", "Size")

# Creamos la columna Datetime, que incluye fecha y hora, partiendo de la columna _c3.
nasaLogs = nasaLogsJulio.withColumn("DateTime", F.to_timestamp(F.col("_c3"), "dd/MMM/yyyy:HH:mm:ss"))

# Eliminamos la columna _c3, ya que obtuvimos lo que deseabamos de ella en el paso anterior.
nasaLogs = nasaLogs.drop("_c3")

# Desglosamos la fecha y la hora de nuestra columna Datetime.
nasaLogs = nasaLogs.withColumn("Date",F.to_date("DateTime"))
nasaLogs = nasaLogs.withColumn('Time', F.date_format('DateTime', 'HH:mm:ss'))
nasaLogs = nasaLogs.drop("DateTime")

# Ordenamos el dataframe para obtener un df final con el que vamos a trabajar las consultas.
nasaLogs = nasaLogs.select("Host","UserIdentifier","UserId","Time","TimeZone","RequestMethod","Resource","Protocol","HttpStatus","Size", "Date")

# El siguiente codigo creo que no es necesario para hacer las pruebas en el servidor. En caso de ser necesario lo descomento.
# El mismo sirve para habilitar el proceso de sobre escritura en particiones en Hive. (eso tengo entendido).
# spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')

# Mediante Spark SQL creamos una base de datos en Hive en la ruta indicada
spark.sql(""" create database if not exists nasaLogs location '/nasaLogs_outputs';""")

# Creamos una tabla dentro de la base de datos antes creada, indicando el esquema y sus tipos, particionada por el campo date y que sea en formato parquet.
spark.sql(""" create table if not exists nasaLogs.nasa_table (Host string,UserIdentifier string,UserId string,Time string,TimeZone string,
RequestMethod string,Resource string,Protocol string,HttpStatus string,Size string) partitioned by (Date date) stored as parquet; """)

# Una vez creada la base de datos y la tabla le insertamos el dataframe que hemos venido trabajando.
nasaLogs.write.mode("overwrite").insertInto("nasaLogs.nasa_table")

# A continuacion una serie de consultas al dataframe y sus respuestas:  

# 1) - ¿Cuáles son los distintos protocolos web utilizados? Agrúpalos.
uno = nasaLogs.groupBy("Protocol").count().limit(1)
uno = uno.withColumn("Consulta", F.lit("Protocolo mas usado"))

# 2) - ¿Cuáles son los códigos de estado más comunes en la web? Agrúpalos y ordénalos para ver cuál es el más común.
dos = nasaLogs.groupBy("HttpStatus").count().sort(F.col("count").desc()).limit(1)
dos = dos.withColumn("Consulta", F.lit("HttpStatus mas comun"))

# 3) - ¿Y los métodos de petición (verbos) más utilizados?
tres = nasaLogs.groupBy("RequestMethod").count().sort(F.col("count").desc()).limit(1)
tres = tres.withColumn("Consulta", F.lit("RequestMethod mas utilizado"))
 
# 4) - ¿Qué recurso tuvo la mayor transferencia de bytes de la página web?
cuatro = nasaLogs.select(F.col("Resource"), F.col("Size")).orderBy(F.col("Size").desc()).limit(1)
cuatro = cuatro.withColumn("Consulta", F.lit("Resource con mas transferencia de bytes"))

# 5) - Además, queremos saber que recurso de nuestra web es el que más tráfico recibe. Es decir, el recurso con más registros en nuestro log.
cinco = nasaLogs.groupBy("Resource").count().sort(F.col("count").desc()).limit(1)
cinco = cinco.withColumn("Consulta", F.lit("Resource con mas registros"))

# 6) - ¿Qué días la web recibió más tráfico?
seis = nasaLogs.select(F.date_format("Date","d").alias("Dia"))\
                                .groupBy(F.col("Dia"))\
                                .agg(F.count("*").alias("Trafico"))\
                                .orderBy(F.desc(F.col("Trafico")))\
                                .limit(1)
seis = seis.withColumn("Consulta", F.lit("Dia con mas trafico"))

# 7) - ¿Cuáles son los hosts más frecuentes?
siete = nasaLogs.groupBy("Host").count().sort(F.col("count").desc()).limit(1)
siete = siete.withColumn("Consulta", F.lit("Host mas frecuente"))

# 8) - ¿A qué horas se produce el mayor número de tráfico en la web?
ocho = nasaLogs.select(F.date_format("Time","H").alias("Hora"))\
                                .groupBy(F.col("Hora"))\
                                .agg(F.count("*").alias("Trafico"))\
                                .orderBy(F.desc(F.col("Trafico")))\
                                .limit(1)
ocho = ocho.withColumn("Consulta", F.lit("Hora con mayor trafico"))

# 9) - ¿Cuál es el número de errores 404 que ha habido cada día?
nueve = nasaLogs.select(F.date_format("Date","d").alias("Dia"),F.col("HttpStatus"))\
                                .where(F.col("HttpStatus") == 404)\
                                .groupBy(F.col("Dia"))\
                                .agg(F.count("*").alias("Errores"))\
                                .orderBy(F.desc(F.col("Errores")))\
                                .limit(1)
nueve = nueve.withColumn("Consulta", F.lit("Dia con mayor numero de error 404"))

# Unimos las consultas obtenidas en un unico df
diez = uno.union(dos)
once = diez.union(tres)
doce = once.union(cuatro)
trece = doce.union(cinco)
catorce = trece.union(seis)
quince = catorce.union(siete)
dieciseis = quince.union(ocho)
diecisiete = dieciseis.union(nueve)

# Renombramos y ordenamos
dieciocho = diecisiete.withColumnRenamed("Protocol","Respuesta")
dieciocho = dieciocho.select("Consulta", "Respuesta", "count")

# crear tabla con location y luego insertarle los datos a la ruta con write parquet. -> hacerla external.
spark.sql(""" create external table if not exists nasaLogs.nasa_answers (Consulta string,Respuesta string,count string) stored as parquet location '/nasaLogs_outputs/nasaLogs_answers'; """)

dieciocho.write.option("header", True) \
      .format("parquet") \
      .mode("overwrite") \
      .save("/nasaLogs_outputs/nasaLogs_answers")

