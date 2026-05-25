from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, max, min, count, sum, stddev, col, round


spark = SparkSession.builder \
    .appName("Analisis SQL Logs y Metricas de Servidores") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


jdbc_url = "jdbc:mysql://mysql-local:3306/monitoreo_servidores"

connection_properties = {
    "user": "root",
    "password": "root123",
    "driver": "com.mysql.cj.jdbc.Driver"
}

df = spark.read.jdbc(
    url=jdbc_url,
    table="logs_metricas_servidores",
    properties=connection_properties
)


print("\n==================================================")
print(" ANALISIS SQL / MYSQL - LOGS Y METRICAS DE SERVIDORES")
print("==================================================\n")


print("=== ESQUEMA DEL DATAFRAME ===")
df.printSchema()


print("\n=== PRIMEROS 10 REGISTROS RESUMIDOS ===")
df.select(
    "id_log",
    "timestamp_evento",
    "servidor",
    "servicio",
    "tipo_evento",
    "nivel",
    "tiempo_respuesta_ms",
    "uso_cpu_porcentaje",
    "uso_ram_porcentaje",
    "latencia_red_ms"
).show(10, truncate=False)


print("\n=== TOTAL DE REGISTROS ===")
total_registros = df.count()
print(f"Total de registros procesados: {total_registros}")


print("\n=== PROMEDIO DE CPU, RAM Y DISCO POR SERVIDOR ===")
df.groupBy("servidor").agg(
    round(avg("uso_cpu_porcentaje"), 2).alias("cpu_promedio"),
    round(avg("uso_ram_porcentaje"), 2).alias("ram_promedio"),
    round(avg("uso_disco_porcentaje"), 2).alias("disco_promedio")
).orderBy("servidor").show(truncate=False)


print("\n=== ESTADISTICAS DE TIEMPO DE RESPUESTA POR NIVEL ===")
df.groupBy("nivel").agg(
    count("*").alias("total_eventos"),
    round(avg("tiempo_respuesta_ms"), 2).alias("tiempo_promedio_ms"),
    min("tiempo_respuesta_ms").alias("tiempo_minimo_ms"),
    max("tiempo_respuesta_ms").alias("tiempo_maximo_ms"),
    round(stddev("tiempo_respuesta_ms"), 2).alias("desviacion_tiempo_ms")
).orderBy("nivel").show(truncate=False)


print("\n=== TOTAL DE ERRORES POR SERVIDOR ===")
df.groupBy("servidor").agg(
    sum("errores_minuto").alias("total_errores")
).orderBy(col("total_errores").desc()).show(truncate=False)


print("\n=== LATENCIA PROMEDIO POR SERVICIO ===")
df.groupBy("servicio").agg(
    round(avg("latencia_red_ms"), 2).alias("latencia_promedio_ms")
).orderBy(col("latencia_promedio_ms").desc()).show(truncate=False)


print("\n=== TOTAL DE EVENTOS POR NIVEL ===")
df.groupBy("nivel").agg(
    count("*").alias("total_eventos")
).orderBy(col("total_eventos").desc()).show(truncate=False)


print("\n=== TOTAL DE EVENTOS POR TIPO ===")
df.groupBy("tipo_evento").agg(
    count("*").alias("total_eventos")
).orderBy(col("total_eventos").desc()).show(truncate=False)


print("\n=== SERVICIOS CON MAYOR TIEMPO DE RESPUESTA PROMEDIO ===")
df.groupBy("servicio").agg(
    round(avg("tiempo_respuesta_ms"), 2).alias("tiempo_promedio_ms"),
    max("tiempo_respuesta_ms").alias("tiempo_maximo_ms")
).orderBy(col("tiempo_promedio_ms").desc()).show(truncate=False)


print("\n=== RESUMEN GENERAL DE METRICAS NUMERICAS ===")
df.select(
    "tiempo_respuesta_ms",
    "uso_cpu_porcentaje",
    "uso_ram_porcentaje",
    "uso_disco_porcentaje",
    "latencia_red_ms",
    "temperatura_cpu",
    "errores_minuto"
).describe().show(truncate=False)


print("\n==================================================")
print(" ANALISIS SQL / MYSQL FINALIZADO CORRECTAMENTE")
print("==================================================\n")


spark.stop()
