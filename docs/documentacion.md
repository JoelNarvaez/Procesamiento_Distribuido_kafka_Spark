# Documentacion del proceso de desarrollo

Este documento describe paso a paso todo el proceso que se siguió para construir el sistema distribuido de monitoreo y análisis de logs y métricas de servidores.

---

## 1. Instalacion inicial del proyecto

### Creación de la estructura de carpetas

Se creó la estructura completa del proyecto con los siguientes comandos:

```bash
mkdir -p data/raw data/processed data/generator
mkdir -p kafka/producers kafka/consumers kafka/topics kafka/config
mkdir -p spark/jobs spark/output spark/config
mkdir -p docker/local docker/cluster/nodo1 docker/cluster/nodo2 docker/cluster/nodo3
mkdir -p database docs
```

### Creación de archivos base

```bash
touch data/raw/logs_metricas.csv
touch data/raw/logs_metricas.json
touch data/raw/logs_metricas.sql
touch data/generator/generar_logs.py

touch kafka/producers/producer_logs.js
touch kafka/consumers/consumer_logs.js
touch kafka/topics/crear_topicos.sh
touch kafka/package.json

touch spark/jobs/analisis_csv.py
touch spark/jobs/analisis_json.py
touch spark/jobs/analisis_sql.py

touch docker/local/docker-compose.yml
touch docker/cluster/nodo1/docker-compose.yml
touch docker/cluster/nodo2/docker-compose.yml
touch docker/cluster/nodo3/docker-compose.yml

touch database/schema.sql
touch database/inserts.sql

touch docs/documentacion.md
touch docs/alcance.md
touch docs/arquitectura.md
touch docs/pruebas.md
touch docs/resultados.md

touch README.md
touch requirements.txt
touch .gitignore
```

### Permisos de ejecución

Se dio permiso de ejecución al script de creación de tópicos:

```bash
chmod +x kafka/topics/crear_topicos.sh
```

---

## 2. Configuracion del entorno local con Docker

### Levantamiento del entorno

Se utilizó Docker Compose para levantar todos los servicios del entorno local:

```bash
docker compose up -d
```

Los servicios que se esperan corriendo son:

| Contenedor         | Servicio                    |
|--------------------|-----------------------------|
| kafka-local        | Kafka Broker (modo KRaft)   |
| spark-master-local | Spark Master                |
| spark-worker-local | Spark Worker                |
| mysql-local        | MySQL 8                     |

### Verificación de contenedores

Para verificar que todos los contenedores estén corriendo:

```bash
docker compose ps
```

También se puede usar:

```bash
docker ps
```

---

## 3. Validacion de Kafka local

Kafka se ejecuta como un nodo local en modo KRaft (sin ZooKeeper).

### Entrada al contenedor

```bash
docker exec -it kafka-local bash
```

### Listar tópicos

```bash
/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Si el comando no muestra errores, Kafka está funcionando correctamente.

---

## 4. Validacion de Spark local

Spark debe mostrar un Master y un Worker conectado.

### URL de la interfaz web

```
http://localhost:8080
```

En la interfaz debe aparecer al menos un Worker conectado al Master.

### Revisión de logs

Para revisar si hay errores en los contenedores de Spark:

```bash
docker logs spark-master-local --tail 80
docker logs spark-worker-local --tail 80
```

---

## 5. Validacion de MySQL local

### Entrada al contenedor

```bash
docker exec -it mysql-local mysql -u root -p
```

Contraseña configurada: `root123`

### Comandos de verificación dentro de MySQL

```sql
SHOW DATABASES;
USE monitoreo_servidores;
SHOW TABLES;
DESCRIBE logs_metricas_servidores;
```

Se espera que exista la base de datos `monitoreo_servidores` y la tabla `logs_metricas_servidores`.

---

## 6. Creacion de topicos en Kafka

Después de comprobar que los contenedores funcionaban correctamente, lo siguiente fue crear los tópicos reales de Kafka.

### Creación del script

Se creó el archivo `kafka/topics/crear_topicos.sh`. Este script contiene la lógica para crear los cinco tópicos del proyecto utilizando las herramientas internas de Kafka. Define el servidor de Kafka, la lista de tópicos y los parámetros de particiones y factor de replicación.

### Permisos de ejecución

Se dieron permisos de ejecución al script desde la raíz del proyecto:

```bash
chmod +x kafka/topics/crear_topicos.sh
```

### Copia del script al contenedor

Como el script usa herramientas internas de Kafka ubicadas en `/opt/kafka/bin/`, se copió el script dentro del contenedor:

```bash
docker cp kafka/topics/crear_topicos.sh kafka-local:/tmp/crear_topicos.sh
```

### Entrada al contenedor

```bash
docker exec -it kafka-local bash
```

### Ejecución del script

Dentro del contenedor se dieron permisos al script copiado y se ejecutó:

```bash
chmod +x /tmp/crear_topicos.sh
/tmp/crear_topicos.sh
```

Con eso se crearon los cinco tópicos del proyecto:

- `metricas_recursos`
- `logs_http`
- `logs_errores`
- `metricas_red`
- `logs_seguridad`

### Verificación de los tópicos

Para verificar que se crearon correctamente:

```bash
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

Desde fuera del contenedor:

```bash
docker exec -it kafka-local /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

Para revisar los detalles de un tópico específico:

```bash
docker exec -it kafka-local /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic metricas_recursos
```

### Configuración utilizada

En el entorno local se usó:

```
--partitions 3
--replication-factor 1
```

Se usó factor de replicación 1 porque localmente solo existe un broker Kafka. En la versión final distribuida, cuando haya tres nodos físicos, el factor de replicación se cambiará a `3`.

---

## 7. Configuracion del productor Kafka con Node.js

Después de crear los tópicos en Kafka, lo siguiente fue configurar la parte de Node.js para que pudiera enviar mensajes reales a los tópicos.

### Configuración de `kafka/package.json`

Se configuró el archivo `kafka/package.json` con la información del proyecto y la dependencia principal `kafkajs`, que es la librería cliente de Kafka para Node.js.

El archivo define dos scripts:

- `producer`: ejecuta el productor.
- `consumer`: ejecuta el consumidor.

### Instalación de dependencias

Desde la carpeta `kafka`:

```bash
cd kafka
npm install
```

Esto creó:

- `kafka/node_modules/`
- `kafka/package-lock.json`

> **Nota:** La carpeta `node_modules/` debe estar incluida en `.gitignore` para no subirla al repositorio.

### Creación del productor

Se creó el archivo `kafka/producers/producer_logs.js`. Este productor utiliza la librería `kafkajs` para conectarse al broker de Kafka local en `localhost:9092`. Internamente realiza lo siguiente:

1. Define la lista de los cinco tópicos del proyecto.
2. Genera eventos simulados con campos como servidor, IP, servicio, tipo de evento, nivel, código de estado, endpoint, usuario, ciudad y métricas técnicas.
3. Asigna métricas coherentes según el nivel del evento (`INFO`, `WARNING`, `ERROR`, `CRITICAL`), siguiendo la misma estrategia de rangos controlados del generador de datos.
4. Selecciona automáticamente el tópico correspondiente según el tipo de evento generado.
5. Envía 20 mensajes de prueba en formato JSON hacia los tópicos correspondientes.

### Ejecución del productor

Desde la carpeta `kafka`:

```bash
npm run producer
```

O directamente:

```bash
node producers/producer_logs.js
```

Si todo funciona correctamente, aparecen mensajes como:

```
Productor conectado a Kafka
Mensaje enviado al tópico logs_http: ...
Mensaje enviado al tópico metricas_recursos: ...
Mensaje enviado al tópico logs_errores: ...
Productor desconectado
```

### Verificación desde Kafka

Para comprobar que los mensajes llegaron a los tópicos, se usó un consumidor de consola dentro del contenedor:

```bash
docker exec -it kafka-local /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic logs_http \
  --from-beginning
```

Si hay mensajes en el tópico, aparecen como JSON. Para salir: `Ctrl + C`.

Para revisar otros tópicos, solo se cambia el nombre:

```bash
docker exec -it kafka-local /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic metricas_recursos \
  --from-beginning
```

```bash
docker exec -it kafka-local /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic logs_errores \
  --from-beginning
```

---

## 8. Configuracion del consumidor Kafka con Node.js

Después de verificar que el productor enviaba mensajes correctamente, el siguiente paso fue crear el consumidor en Node.js para leer los mensajes directamente desde el código, sin depender del consumidor de consola del contenedor Kafka.

### Creación del consumidor

Se creó el archivo `kafka/consumers/consumer_logs.js`. Este consumidor utiliza la librería `kafkajs` para conectarse al broker Kafka en `localhost:9092`. Internamente realiza lo siguiente:

1. Se conecta al broker como parte del grupo de consumidores `grupo-monitoreo-servidores`.
2. Se suscribe a los cinco tópicos del proyecto.
3. Lee los mensajes desde el principio (`fromBeginning: true`).
4. Parsea el contenido JSON de cada mensaje recibido.
5. Imprime en consola información del mensaje: tópico, partición, offset y los campos principales del evento (servidor, servicio, nivel, CPU, RAM, latencia, mensaje).

### Ejecución del consumidor

Terminal 1 (consumidor):

```bash
cd ~/Documents/proyecto-final-distribuido/kafka
npm run consumer
```

Se deja corriendo en esa terminal.

### Ejecución del productor en otra terminal

Terminal 2 (productor):

```bash
cd ~/Documents/proyecto-final-distribuido/kafka
npm run producer
```

### Salida esperada del consumidor

Si todo funciona correctamente, en la terminal del consumidor aparecen mensajes como:

```
Consumidor conectado a Kafka
Suscrito al tópico: metricas_recursos
Suscrito al tópico: logs_http
Suscrito al tópico: logs_errores
Suscrito al tópico: metricas_red
Suscrito al tópico: logs_seguridad
====================================
Tópico: logs_http
Partición: 0
Offset: 0
ID Log: 1
Servidor: server-01
Servicio: api-gateway
Tipo evento: request
Nivel: INFO
CPU: 45.7%
RAM: 62.3%
Latencia: 24 ms
Mensaje: Evento INFO generado por request
```

Para detener el consumidor: `Ctrl + C`

### Lo que se comprobó con esta prueba

Con esta prueba quedó demostrado el flujo completo entre productor, tópicos y consumidor en Kafka:

```
Producer Kafka → envía eventos de logs y métricas
Kafka topics → almacenan los mensajes por tipo de evento
Consumer Kafka → lee los mensajes desde los tópicos
```

El consumidor se mantiene suscrito a los cinco tópicos del proyecto. Esto confirma que el ciclo de mensajería Kafka funciona correctamente en el entorno local y que el sistema puede transmitir y recibir los eventos generados por los servidores simulados.

---

## 9. Analisis de datos CSV con Apache Spark

Después de levantar Spark en el entorno local, el siguiente paso fue ejecutar el primer job de análisis sobre los datos generados en formato CSV.

### Script de análisis

Se creó el archivo `spark/jobs/analisis_csv.py`. Este script utiliza PySpark para leer el archivo `logs_metricas.csv` (con los 100,000 registros generados) y realizar varios análisis estadísticos. Internamente hace lo siguiente:

1. Crea una sesión de Spark conectada al clúster.
2. Lee el CSV detectando el esquema automáticamente.
3. Imprime el esquema del DataFrame y los primeros registros como muestra.
4. Calcula el total de registros procesados.
5. Obtiene promedios de CPU, RAM y disco por servidor.
6. Calcula estadísticas de tiempo de respuesta por nivel (promedio, mínimo, máximo y desviación estándar).
7. Suma los errores totales por servidor.
8. Obtiene la latencia promedio por servicio.
9. Cuenta eventos por nivel y por tipo.
10. Suma peticiones por servicio y tráfico total por servidor.
11. Lista los eventos críticos y los servidores con uso crítico de recursos.
12. Identifica los servicios con mayor tiempo de respuesta promedio.
13. Genera un resumen general de métricas numéricas.

### Ejecución del job

```bash
docker exec -it spark-master-local /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/analisis_csv.py
```

### Problema inicial: salida desordenada

Al ejecutar el job por primera vez, la salida se veía saturada por dos razones:

**1. Logs internos de Spark.** Spark imprime muchos mensajes informativos por defecto:

```
INFO SparkContext
INFO DAGScheduler
INFO TaskSchedulerImpl
INFO MemoryStore
INFO CodeGenerator
WARN NativeCodeLoader
```

Estos no son errores ni resultados, sino mensajes internos que indican que Spark creó jobs, stages, tareas, particiones, memoria, conexiones y ejecuciones del worker. El job sí funcionaba: leía los 100,000 registros y mostraba los resultados, pero quedaban escondidos entre los logs.

**2. Tablas muy anchas.** El DataFrame tiene más de 20 columnas, por lo que `df.show(10, truncate=False)` imprimía una tabla que se desbordaba horizontalmente en la terminal.

### Primera mejora dentro del script

Se agregó esta línea justo después de crear la sesión Spark para ocultar la mayoría de mensajes `INFO` y `WARN`:

```python
spark.sparkContext.setLogLevel("ERROR")
```

También se reemplazó la impresión completa del DataFrame por una selección de columnas más importantes:

- `id_log`
- `timestamp_evento`
- `servidor`
- `servicio`
- `tipo_evento`
- `nivel`
- `tiempo_respuesta_ms`
- `uso_cpu_porcentaje`
- `uso_ram_porcentaje`
- `latencia_red_ms`

Además, se aplicó `round(...)` con 2 decimales a los promedios para que las tablas no salieran con números como `41.85837648113117`, sino con `41.86`.

### Logs persistentes antes del inicio del script

Al volver a ejecutar el job, aún seguían apareciendo muchos logs internos. Esto se debe a que `setLogLevel("ERROR")` solo funciona **después** de que la sesión de Spark ya está creada. Pero `spark-submit` imprime varios mensajes **antes** de eso, mientras Java arranca, se conecta al Master y registra el executor.

### Configuración avanzada con log4j2

Para silenciar también los logs iniciales, se creó el archivo `spark/jobs/log4j2.properties`. Este archivo configura los loggers de Spark, Hadoop, Jetty, Netty y Parquet en nivel `error`, de manera que solo se muestren errores reales en la consola.

### Ejecución con configuración limpia

El job ahora se ejecuta pasándole la configuración de log4j2 tanto al driver como al executor:

```bash
docker exec -it spark-master-local /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf "spark.ui.showConsoleProgress=false" \
  --conf "spark.driver.extraJavaOptions=-Dlog4j.configurationFile=/opt/spark/jobs/log4j2.properties" \
  --conf "spark.executor.extraJavaOptions=-Dlog4j.configurationFile=/opt/spark/jobs/log4j2.properties" \
  /opt/spark/jobs/analisis_csv.py
```

Esto reduce casi por completo los logs internos.

### Alternativa: separar logs y resultados

Si aún quedan mensajes iniciales que no se pueden silenciar, se puede redirigir todo lo que Spark imprime como log a un archivo, dejando en la terminal únicamente los resultados del análisis:

```bash
docker exec -it spark-master-local /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf "spark.ui.showConsoleProgress=false" \
  --conf "spark.driver.extraJavaOptions=-Dlog4j.configurationFile=/opt/spark/jobs/log4j2.properties" \
  --conf "spark.executor.extraJavaOptions=-Dlog4j.configurationFile=/opt/spark/jobs/log4j2.properties" \
  /opt/spark/jobs/analisis_csv.py 2> spark/output/logs_spark_csv.txt
```

Con esta forma:

- La salida limpia (resultados del análisis) queda visible en la terminal.
- Los logs técnicos se guardan en `spark/output/logs_spark_csv.txt` por si se necesitan revisar después.

### Resultado

Con estas configuraciones, el job de análisis CSV produce una salida ordenada y presentable. Spark procesa los 100,000 registros del archivo y muestra los análisis estadísticos esperados sin mezclarlos con mensajes internos del framework.

---

## 10. Analisis de datos JSON con Apache Spark

Después de comprobar que Spark procesaba correctamente los datos en formato CSV, el siguiente paso fue validar la fuente de datos JSON.

### Script de análisis JSON

Se creó el archivo `spark/jobs/analisis_json.py`. Este script tiene la misma estructura que el de CSV, pero adaptado para leer el archivo `logs_metricas.json` mediante `spark.read.json(...)` con la opción `multiline` activada.

Realiza los mismos análisis estadísticos:

1. Esquema del DataFrame.
2. Total de registros procesados.
3. Promedio de CPU, RAM y disco por servidor.
4. Estadísticas de tiempo de respuesta por nivel.
5. Total de errores por servidor.
6. Latencia promedio por servicio.
7. Conteo de eventos por nivel y tipo.
8. Eventos críticos.
9. Servicios con mayor tiempo de respuesta promedio.
10. Resumen general de métricas numéricas.

### Ejecución del job JSON

Se utiliza el mismo `log4j2.properties` configurado para el análisis CSV:

```bash
docker exec -it spark-master-local /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf "spark.ui.showConsoleProgress=false" \
  --conf "spark.driver.extraJavaOptions=-Dlog4j.configurationFile=/opt/spark/jobs/log4j2.properties" \
  --conf "spark.executor.extraJavaOptions=-Dlog4j.configurationFile=/opt/spark/jobs/log4j2.properties" \
  /opt/spark/jobs/analisis_json.py
```

### Resultado esperado

La salida debe mostrar:

```
ANALISIS JSON - LOGS Y METRICAS DE SERVIDORES
ESQUEMA DEL DATAFRAME
TOTAL DE REGISTROS
Total de registros procesados: 100000
```

Si el total es `100000`, queda comprobado que Spark procesa correctamente los datos JSON.

---

## 11. Analisis de datos SQL (MySQL) con Apache Spark

El flujo de esta tercera fuente es:

```
logs_metricas.sql / inserts.sql → MySQL → Spark (JDBC) → Análisis estadísticos
```

### Verificación de datos en MySQL

Aunque el esquema ya estaba creado con `schema.sql`, era necesario verificar que la tabla tuviera los 100,000 registros:

```bash
docker exec -it mysql-local mysql -u root -p
```

Contraseña: `root123`

Dentro de MySQL:

```sql
USE monitoreo_servidores;
SELECT COUNT(*) FROM logs_metricas_servidores;
```

### Carga de los inserts

Si el conteo devolvía `0`, se cargó el archivo de inserts:

```bash
docker exec -i mysql-local mysql -u root -proot123 monitoreo_servidores < database/inserts.sql
```

Para verificar la carga:

```bash
docker exec -it mysql-local mysql -u root -proot123 -e "USE monitoreo_servidores; SELECT COUNT(*) FROM logs_metricas_servidores;"
```

Debe regresar `100000`.

### Driver JDBC de MySQL para Spark

Para que Spark pueda leer MySQL, necesita el conector JDBC. Se usa el paquete:

```
com.mysql:mysql-connector-j:8.4.0
```

Este se pasa como parámetro `--packages` al ejecutar `spark-submit`.

### Script de análisis SQL

Se creó el archivo `spark/jobs/analisis_sql.py`. A diferencia de los scripts de CSV y JSON, este se conecta a MySQL por JDBC. Internamente hace lo siguiente:

1. Define la URL JDBC apuntando a `mysql-local:3306/monitoreo_servidores`.
2. Configura las propiedades de conexión (usuario `root`, contraseña `root123` y driver `com.mysql.cj.jdbc.Driver`).
3. Lee la tabla `logs_metricas_servidores` directamente con `spark.read.jdbc(...)`.
4. Ejecuta los mismos análisis estadísticos que las versiones de CSV y JSON.

### Ejecución del job SQL

```bash
docker exec -it spark-master-local /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages com.mysql:mysql-connector-j:8.4.0 \
  --conf "spark.ui.showConsoleProgress=false" \
  --conf "spark.driver.extraJavaOptions=-Dlog4j.configurationFile=/opt/spark/jobs/log4j2.properties" \
  --conf "spark.executor.extraJavaOptions=-Dlog4j.configurationFile=/opt/spark/jobs/log4j2.properties" \
  /opt/spark/jobs/analisis_sql.py
```

Debe mostrar:

```
ANALISIS SQL / MYSQL - LOGS Y METRICAS DE SERVIDORES
TOTAL DE REGISTROS
Total de registros procesados: 100000
```

### Solución de problemas de conexión

Si aparece un error de conexión entre Spark y MySQL, hay que verificar que ambos contenedores estén en la misma red de Docker:

```bash
docker inspect spark-master-local | grep proyecto_net
docker inspect mysql-local | grep proyecto_net
```

También se puede probar la conectividad desde dentro del contenedor Spark:

```bash
docker exec -it spark-master-local bash
```

Dentro del contenedor:

```bash
ping mysql-local
```

Si `ping` no está disponible:

```bash
getent hosts mysql-local
```

Debe resolver una IP interna de Docker.

---

## 12. Resultado de las tres fuentes de datos

Con las tres pruebas completadas quedaron comprobadas las tres fuentes de datos del proyecto:

| Fuente | Formato         | Método de lectura        | Registros |
|--------|-----------------|--------------------------|-----------|
| CSV    | Archivo plano   | `spark.read.csv(...)`    | 100,000   |
| JSON   | Semiestructurado| `spark.read.json(...)`   | 100,000   |
| SQL    | Base de datos   | `spark.read.jdbc(...)`   | 100,000   |

Esto confirma que el sistema puede leer y procesar datos desde archivos planos (CSV), datos semiestructurados (JSON) y bases de datos relacionales (MySQL mediante JDBC).

---

## 13. Pruebas comprobables en el entorno local

En el entorno local, antes de migrar al ambiente distribuido, se pueden comprobar los siguientes aspectos:

### Pruebas de Spark

- Que el Spark Master levanta correctamente.
- Que el Spark Worker se conecta al Master.
- Que `spark-submit` envía trabajos al Master.
- Que el Worker ejecuta las tareas asignadas.

### Pruebas de Kafka

- Que Kafka recibe mensajes enviados por los productores.
- Que Kafka conserva los mensajes dentro de los tópicos.
- Que el consumidor puede leer los mensajes desde los tópicos.

### Importancia del entorno local

Aunque todas estas pruebas se ejecutan en una sola máquina mediante contenedores Docker, permiten validar que cada componente del sistema funciona individualmente y que la comunicación entre ellos es correcta.

Esto es importante porque, antes de pasar al entorno distribuido en tres máquinas físicas, conviene asegurarse de que el código, los scripts, los productores, los consumidores y los jobs de Spark funcionan sin errores. De esta manera, en la migración al clúster físico solo se modifican las configuraciones de red (IPs, brokers, Master) y no la lógica del sistema.

### Lo que no se puede comprobar en local

En un entorno con un solo broker Kafka y un solo Worker Spark no se pueden probar:

- Replicación real entre nodos.
- Tolerancia a fallos ante la caída de un nodo físico.
- Distribución de particiones entre brokers reales.
- Procesamiento paralelo entre Workers separados.
- Comportamiento del líder y réplicas ante desconexiones.

Estas pruebas se reservan para el entorno distribuido en las tres máquinas físicas.

---

## 14. Ejemplo de registro JSON

Para referencia, cada registro generado tiene la siguiente estructura:

```json
{
  "id_log": 1,
  "timestamp_evento": "2026-06-01 10:35:22",
  "servidor": "server-01",
  "ip_servidor": "192.168.1.101",
  "servicio": "api-gateway",
  "tipo_evento": "request",
  "nivel": "INFO",
  "codigo_estado": 200,
  "endpoint": "/api/productos",
  "usuario": "usuario_demo",
  "ciudad": "Aguascalientes",
  "tiempo_respuesta_ms": 145,
  "uso_cpu_porcentaje": 45.7,
  "uso_ram_porcentaje": 62.3,
  "uso_disco_porcentaje": 58.9,
  "bytes_entrada": 2048,
  "bytes_salida": 8192,
  "peticiones_por_minuto": 320,
  "conexiones_activas": 87,
  "errores_minuto": 2,
  "latencia_red_ms": 24,
  "temperatura_cpu": 61.3,
  "mensaje": "Solicitud procesada correctamente"
}
```
