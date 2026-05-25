# Proyecto Final de Procesamiento Distribuido

## Sistema distribuido para monitoreo y análisis de logs y métricas de servidores mediante Apache Kafka y Apache Spark

## 1. Descripción

Este proyecto tiene como finalidad implementar un sistema distribuido para simular, transmitir y analizar logs y métricas generadas por servidores.

La solución utiliza Apache Kafka para la transmisión de eventos en tiempo real y Apache Spark para el procesamiento distribuido de grandes volúmenes de datos.

El sistema se desarrollará primero en un entorno local contenerizado con Docker y posteriormente se migrará a un entorno distribuido compuesto por tres máquinas físicas conectadas en red local.

## 2. Objetivo general

Diseñar e implementar un sistema distribuido utilizando Apache Kafka, Apache Spark y Docker, capaz de recolectar, transmitir y procesar logs y métricas de servidores, demostrando conceptos como particionamiento, replicación, tolerancia a fallos, procesamiento paralelo y distribución de carga.

## 3. Tecnologías utilizadas

- Apache Kafka.
- Apache Spark.
- Docker.
- Docker Compose.
- JavaScript / Node.js.
- Python / PySpark.
- Faker.
- SQL.
- Git.
- Visual Studio Code.
- Red local con IPs fijas.

## 4. Tema del proyecto

El tema seleccionado es el monitoreo distribuido de logs y métricas de servidores.

Los datos simulados representan eventos generados por servidores, servicios web, APIs, sistemas de autenticación, red y recursos de hardware.

El sistema manejará datos como:

- Uso de CPU.
- Uso de RAM.
- Uso de disco.
- Tiempo de respuesta.
- Latencia de red.
- Bytes enviados.
- Bytes recibidos.
- Peticiones por minuto.
- Conexiones activas.
- Errores por minuto.
- Temperatura del CPU.
- Códigos de estado HTTP.
- Mensajes de log.

## 5. Estructura del proyecto

```text
proyecto-final-distribuido/
├── data/
│   ├── raw/
│   │   ├── logs_metricas.csv
│   │   ├── logs_metricas.json
│   │   └── logs_metricas.sql
│   ├── processed/
│   └── generator/
│       └── generar_logs.py
│
├── kafka/
│   ├── producers/
│   │   └── producer_logs.js
│   ├── consumers/
│   │   └── consumer_logs.js
│   ├── topics/
│   │   └── crear_topicos.sh
│   ├── config/
│   └── package.json
│
├── spark/
│   ├── jobs/
│   │   ├── analisis_csv.py
│   │   ├── analisis_json.py
│   │   └── analisis_sql.py
│   ├── output/
│   └── config/
│
├── docker/
│   ├── local/
│   │   └── docker-compose.yml
│   └── cluster/
│       ├── nodo1/
│       │   └── docker-compose.yml
│       ├── nodo2/
│       │   └── docker-compose.yml
│       └── nodo3/
│           └── docker-compose.yml
│
├── database/
│   ├── schema.sql
│   └── inserts.sql
│
├── docs/
│   ├── documentacion.md
│   ├── alcance.md
│   ├── arquitectura.md
│   ├── pruebas.md
│   └── resultados.md
│
├── requirements.txt
├── .gitignore
└── README.md
```

## 6. Descripción de carpetas

### `data/`

Contiene los datos utilizados por el proyecto.

- `raw/`: datos originales generados en CSV, JSON y SQL.
- `processed/`: resultados generados por Spark.
- `generator/`: script encargado de generar los datos de prueba.

### `kafka/`

Contiene los elementos relacionados con Apache Kafka.

- `producers/`: productores que envían mensajes a Kafka.
- `consumers/`: consumidores que leen mensajes desde Kafka.
- `topics/`: scripts para crear tópicos.
- `config/`: archivos de configuración relacionados con Kafka.
- `package.json`: dependencias de Node.js para productores y consumidores.

### `spark/`

Contiene los scripts de procesamiento con Apache Spark.

- `jobs/`: scripts PySpark para procesar CSV, JSON y SQL.
- `output/`: resultados generados por los trabajos Spark.
- `config/`: configuraciones adicionales de Spark.

### `docker/`

Contiene los archivos de Docker Compose.

- `local/`: entorno local para pruebas en una sola máquina.
- `cluster/`: configuración distribuida para tres nodos físicos.

### `database/`

Contiene los scripts SQL del proyecto.

- `schema.sql`: creación de tablas.
- `inserts.sql`: inserción de datos.

### `docs/`

Contiene la documentación del proyecto.

- `documentacion.md`: documentación general.
- `alcance.md`: alcance del sistema.
- `arquitectura.md`: arquitectura técnica.
- `pruebas.md`: pruebas realizadas.
- `resultados.md`: resultados obtenidos.

## 7. Arquitectura general

La arquitectura final estará formada por tres máquinas físicas conectadas en red local.

```text
Nodo 1
- Kafka Broker + Controller 1
- Spark Master
- IP sugerida: 192.168.1.101

Nodo 2
- Kafka Broker + Controller 2
- Spark Worker 1
- IP sugerida: 192.168.1.102

Nodo 3
- Kafka Broker + Controller 3
- Spark Worker 2
- IP sugerida: 192.168.1.103
```

## 8. Tópicos Kafka

Se crearán al menos cinco tópicos:

- `metricas_recursos`
- `logs_http`
- `logs_errores`
- `metricas_red`
- `logs_seguridad`

Cada tópico tendrá particiones y factor de replicación.

## 9. Modelo de datos

Cada registro representa un evento de monitoreo generado por un servidor o servicio.

Campos principales:

- `id_log`
- `timestamp_evento`
- `servidor`
- `ip_servidor`
- `servicio`
- `tipo_evento`
- `nivel`
- `codigo_estado`
- `endpoint`
- `usuario`
- `ciudad`
- `tiempo_respuesta_ms`
- `uso_cpu_porcentaje`
- `uso_ram_porcentaje`
- `uso_disco_porcentaje`
- `bytes_entrada`
- `bytes_salida`
- `peticiones_por_minuto`
- `conexiones_activas`
- `errores_minuto`
- `latencia_red_ms`
- `temperatura_cpu`
- `mensaje`

## 10. Ejemplo de registro JSON

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

## 11. Flujo del sistema

### Flujo Kafka

```text
Servidor simulado
        ↓
Productor Kafka
        ↓
Tópico Kafka
        ↓
Particiones replicadas
        ↓
Consumidor Kafka
```

### Flujo Spark

```text
Datos CSV / JSON / SQL
        ↓
Apache Spark
        ↓
Procesamiento distribuido
        ↓
Resultados estadísticos
```

## 12. Análisis con Spark

Los trabajos de Spark permitirán obtener estadísticas como:

- Promedio de uso de CPU por servidor.
- Promedio de uso de RAM por servicio.
- Máximo uso de disco por servidor.
- Promedio de tiempo de respuesta.
- Total de errores por servidor.
- Latencia promedio de red.
- Total de bytes enviados y recibidos.
- Servidores con métricas críticas.
- Comparación entre procesamiento local y distribuido.

## 13. Requisitos generales

Para ejecutar el proyecto se requiere:

- Docker instalado.
- Docker Compose instalado.
- Node.js instalado.
- Python instalado.
- Visual Studio Code.
- Git.
- Tres máquinas físicas para la versión distribuida.
- Red local con IPs fijas o reservadas.

## 14. Dependencias de Python

Para la generación de datos se utiliza la librería Faker.

El archivo `requirements.txt` debe contener:

```txt
Faker
```

Para instalar las dependencias se recomienda usar un entorno virtual:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Para ejecutar el generador de datos:

```bash
python data/generator/generar_logs.py
```

> **Nota:** No se recomienda instalar paquetes directamente en el Python global del sistema. En distribuciones recientes de Ubuntu o Linux Mint puede aparecer el error `externally-managed-environment`, por lo que se debe usar un entorno virtual.

## 15. Instalación inicial del proyecto

Crear la estructura de carpetas:

```bash
mkdir -p data/raw data/processed data/generator
mkdir -p kafka/producers kafka/consumers kafka/topics kafka/config
mkdir -p spark/jobs spark/output spark/config
mkdir -p docker/local docker/cluster/nodo1 docker/cluster/nodo2 docker/cluster/nodo3
mkdir -p database docs
```

Crear archivos base:

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

Dar permiso de ejecución al script de tópicos:

```bash
chmod +x kafka/topics/crear_topicos.sh
```

## 16. Configuracion local

## Levantamiento del entorno

Comando utilizado:

```bash
docker compose up -d
```

Servicios esperados:

```text
kafka-local
spark-master-local
spark-worker-local
mysql-local
```

## Verificación de contenedores

Comando para verificar contenedores:

```bash
docker compose ps
```

También se puede verificar con:

```bash
docker ps
```

## Validación de Kafka local

Kafka debe ejecutarse como un nodo local en modo KRaft.

Comando para entrar al contenedor:

```bash
docker exec -it kafka-local bash
```

Comando para listar tópicos:

```bash
/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Si el comando no muestra errores, Kafka está funcionando correctamente.

## Validación de Spark local

Spark debe mostrar un Master y un Worker conectado.

URL de validación:

```text
http://localhost:8080
```

En la interfaz debe aparecer al menos un Worker conectado al Master.

Contenedores relacionados:

```text
spark-master-local
spark-worker-local
```

Comandos para revisar logs:

```bash
docker logs spark-master-local --tail 80
docker logs spark-worker-local --tail 80
```

## Validación de MySQL local

Comando para entrar al contenedor:

```bash
docker exec -it mysql-local mysql -u root -p
```

Contraseña configurada:

```text
root123
```

Comandos dentro de MySQL:

```sql
SHOW DATABASES;
USE monitoreo_servidores;
SHOW TABLES;
DESCRIBE logs_metricas_servidores;
```

Se espera que exista la base de datos:

```text
monitoreo_servidores
```

Y la tabla:

```text
logs_metricas_servidores
```

# Creación de tópicos en Kafka local

Después de comprobar que los contenedores funcionaban correctamente, lo siguiente fue crear los tópicos reales de Kafka.

## 1. Creación del script

Primero se creó el archivo:

```text
kafka/topics/crear_topicos.sh
```

Este script contiene la lógica para crear los cinco tópicos del proyecto utilizando las herramientas internas de Kafka. Define el servidor de Kafka, la lista de tópicos y los parámetros de particiones y factor de replicación.

## 2. Permisos de ejecución

Se dieron permisos de ejecución al script desde la raíz del proyecto:

```bash
chmod +x kafka/topics/crear_topicos.sh
```

## 3. Copia del script al contenedor

Como el script usa herramientas internas de Kafka ubicadas en `/opt/kafka/bin/`, se copió el script dentro del contenedor Kafka:

```bash
docker cp kafka/topics/crear_topicos.sh kafka-local:/tmp/crear_topicos.sh
```

## 4. Entrada al contenedor

Se entró al contenedor:

```bash
docker exec -it kafka-local bash
```

## 5. Ejecución del script

Dentro del contenedor se dieron permisos al script copiado:

```bash
chmod +x /tmp/crear_topicos.sh
```

Y se ejecutó:

```bash
/tmp/crear_topicos.sh
```

Con eso se crearon los cinco tópicos del proyecto:

```text
metricas_recursos
logs_http
logs_errores
metricas_red
logs_seguridad
```

## 6. Verificación de los tópicos

Para verificar que sí se crearon, se usó:

```bash
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

También se puede verificar desde fuera del contenedor con:

```bash
docker exec -it kafka-local /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

Y para revisar los detalles de un tópico específico:

```bash
docker exec -it kafka-local /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic metricas_recursos
```

## 7. Configuración utilizada

En el entorno local se usó:

```text
--partitions 3
--replication-factor 1
```

Esto se debe a que localmente solo existe un broker Kafka. En la versión final distribuida, cuando haya tres nodos físicos, el factor de replicación se podrá cambiar a `3`.