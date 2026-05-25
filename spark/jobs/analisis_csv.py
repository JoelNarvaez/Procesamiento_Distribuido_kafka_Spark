from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, max, min, count, sum, stddev, col


spark = SparkSession.builder \
    .appName("Analisis CSV Logs y Metricas de Servidores") \
    .getOrCreate()


ruta_csv = "/opt/spark/data/raw/logs_metricas.csv"

df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(ruta_csv)


print("=== ESQUEMA DEL DATAFRAME ===")
df.printSchema()

print("=== PRIMEROS 10 REGISTROS ===")
df.show(10, truncate=False)

print("=== TOTAL DE REGISTROS ===")
print(df.count())

print("=== PROMEDIO DE CPU, RAM Y DISCO POR SERVIDOR ===")
df.groupBy("servidor").agg(
    avg("uso_cpu_porcentaje").alias("cpu_promedio"),
    avg("uso_ram_porcentaje").alias("ram_promedio"),
    avg("uso_disco_porcentaje").alias("disco_promedio")
).show(truncate=False)

print("=== ESTADISTICAS DE TIEMPO DE RESPUESTA POR NIVEL ===")
df.groupBy("nivel").agg(
    count("*").alias("total_eventos"),
    avg("tiempo_respuesta_ms").alias("tiempo_promedio_ms"),
    min("tiempo_respuesta_ms").alias("tiempo_minimo_ms"),
    max("tiempo_respuesta_ms").alias("tiempo_maximo_ms"),
    stddev("tiempo_respuesta_ms").alias("desviacion_tiempo_ms")
).show(truncate=False)

print("=== TOTAL DE ERRORES POR SERVIDOR ===")
df.groupBy("servidor").agg(
    sum("errores_minuto").alias("total_errores")
).orderBy(col("total_errores").desc()).show(truncate=False)

print("=== LATENCIA PROMEDIO POR SERVICIO ===")
df.groupBy("servicio").agg(
    avg("latencia_red_ms").alias("latencia_promedio_ms")
).orderBy(col("latencia_promedio_ms").desc()).show(truncate=False)

print("=== EVENTOS CRITICOS ===")
df.filter(col("nivel") == "CRITICAL").select(
    "id_log",
    "timestamp_evento",
    "servidor",
    "servicio",
    "nivel",
    "uso_cpu_porcentaje",
    "uso_ram_porcentaje",
    "tiempo_respuesta_ms",
    "latencia_red_ms",
    "errores_minuto"
).show(20, truncate=False)

print("=== SERVIDORES CON USO CRITICO DE RECURSOS ===")
df.filter(
    (col("uso_cpu_porcentaje") > 85) |
    (col("uso_ram_porcentaje") > 85) |
    (col("uso_disco_porcentaje") > 85)
).select(
    "id_log",
    "servidor",
    "servicio",
    "nivel",
    "uso_cpu_porcentaje",
    "uso_ram_porcentaje",
    "uso_disco_porcentaje"
).show(20, truncate=False)


spark.stop()