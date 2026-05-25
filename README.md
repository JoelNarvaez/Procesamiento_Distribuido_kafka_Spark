# Proyecto Final de Procesamiento Distribuido

Sistema distribuido para monitoreo y análisis de logs y métricas de servidores mediante Apache Kafka y Apache Spark.

## Descripcion

Sistema que simula, transmite y analiza logs y métricas generadas por servidores. Utiliza Apache Kafka para la transmisión de eventos en tiempo real y Apache Spark para el procesamiento distribuido de grandes volúmenes de datos (100,000 registros).

El sistema se desarrolla primero en un entorno local contenerizado con Docker y posteriormente se migra a un entorno distribuido con tres máquinas físicas conectadas en red local.

## Tecnologias

- **Mensajería:** Apache Kafka (KRaft)
- **Procesamiento:** Apache Spark / PySpark
- **Base de datos:** MySQL
- **Contenedores:** Docker / Docker Compose
- **Productores/Consumidores:** Node.js + KafkaJS
- **Generación de datos:** Python + Faker
- **Formatos de datos:** CSV, JSON, SQL

## Estructura del proyecto

```
proyecto-final-distribuido/
├── data/
│   ├── raw/                  # Datos originales (CSV, JSON, SQL)
│   ├── processed/            # Resultados generados por Spark
│   └── generator/            # Script generador de datos (Faker)
│
├── kafka/
│   ├── producers/            # Productor de eventos (Node.js)
│   ├── consumers/            # Consumidor de eventos (Node.js)
│   ├── topics/               # Script para crear tópicos
│   └── package.json
│
├── spark/
│   ├── jobs/                 # Scripts PySpark (CSV, JSON, SQL)
│   ├── output/               # Resultados de los análisis
│   └── log4j2.properties     # Configuración de logs
│
├── docker/
│   ├── local/                # Docker Compose para entorno local
│   └── cluster/              # Docker Compose para 3 nodos físicos
│
├── database/
│   ├── schema.sql            # Creación de tablas
│   └── inserts.sql           # Inserción de datos
│
├── docs/                     # Documentación del proyecto
├── requirements.txt
├── .gitignore
└── README.md
```

## Arquitectura

### Entorno local (Docker)

| Contenedor         | Servicio                    |
|--------------------|-----------------------------|
| kafka-local        | Kafka Broker (KRaft)        |
| spark-master-local | Spark Master                |
| spark-worker-local | Spark Worker                |
| mysql-local        | MySQL 8                     |

### Entorno distribuido (3 nodos físicos)

| Nodo | IP sugerida    | Servicios                          |
|------|----------------|------------------------------------|
| 1    | 192.168.1.101  | Kafka Broker + Controller, Spark Master |
| 2    | 192.168.1.102  | Kafka Broker + Controller, Spark Worker 1 |
| 3    | 192.168.1.103  | Kafka Broker + Controller, Spark Worker 2 |

## Topicos Kafka

| Tópico             | Descripción                        |
|--------------------|------------------------------------|
| metricas_recursos  | CPU, RAM, disco, temperatura       |
| logs_http          | Peticiones HTTP y códigos de estado|
| logs_errores       | Eventos de error y críticos        |
| metricas_red       | Latencia, bytes, conexiones        |
| logs_seguridad     | Eventos de autenticación           |

Configuración local: 3 particiones, factor de replicación 1.

## Instalacion y ejecucion

### Requisitos previos

- Docker y Docker Compose
- Node.js
- Python 3 con entorno virtual

### 1. Levantar el entorno local

```bash
cd docker/local
docker compose up -d
```

Verificar contenedores:

```bash
docker compose ps
```

### 2. Instalar dependencias de Python

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 3. Generar datos de prueba

```bash
python data/generator/generar_logs.py
```

Esto genera 100,000 registros en los formatos CSV, JSON y SQL.

### 4. Crear topicos en Kafka

```bash
docker cp kafka/topics/crear_topicos.sh kafka-local:/tmp/crear_topicos.sh
docker exec -it kafka-local bash -c "chmod +x /tmp/crear_topicos.sh && /tmp/crear_topicos.sh"
```

Verificar:

```bash
docker exec -it kafka-local /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

### 5. Cargar datos en MySQL

```bash
docker exec -i mysql-local mysql -u root -proot123 monitoreo_servidores < database/inserts.sql
```

Verificar:

```bash
docker exec -it mysql-local mysql -u root -proot123 -e "SELECT COUNT(*) FROM monitoreo_servidores.logs_metricas_servidores;"
```

### 6. Ejecutar productor y consumidor Kafka

Terminal 1 (consumidor):

```bash
cd kafka
npm install
npm run consumer
```

Terminal 2 (productor):

```bash
cd kafka
npm run producer
```

### 7. Ejecutar analisis con Spark

**Análisis CSV:**

```bash
docker exec -it spark-master-local /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf "spark.ui.showConsoleProgress=false" \
  --conf "spark.driver.extraJavaOptions=-Dlog4j.configurationFile=/opt/spark/jobs/log4j2.properties" \
  --conf "spark.executor.extraJavaOptions=-Dlog4j.configurationFile=/opt/spark/jobs/log4j2.properties" \
  /opt/spark/jobs/analisis_csv.py
```

**Análisis JSON:**

```bash
docker exec -it spark-master-local /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf "spark.ui.showConsoleProgress=false" \
  --conf "spark.driver.extraJavaOptions=-Dlog4j.configurationFile=/opt/spark/jobs/log4j2.properties" \
  --conf "spark.executor.extraJavaOptions=-Dlog4j.configurationFile=/opt/spark/jobs/log4j2.properties" \
  /opt/spark/jobs/analisis_json.py
```

**Análisis SQL (MySQL):**

```bash
docker exec -it spark-master-local /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages com.mysql:mysql-connector-j:8.4.0 \
  --conf "spark.ui.showConsoleProgress=false" \
  --conf "spark.driver.extraJavaOptions=-Dlog4j.configurationFile=/opt/spark/jobs/log4j2.properties" \
  --conf "spark.executor.extraJavaOptions=-Dlog4j.configurationFile=/opt/spark/jobs/log4j2.properties" \
  /opt/spark/jobs/analisis_sql.py
```

## Analisis realizados por Spark

Cada job (CSV, JSON, SQL) ejecuta los siguientes análisis sobre los 100,000 registros:

- Promedio de CPU, RAM y disco por servidor
- Estadísticas de tiempo de respuesta por nivel (promedio, min, max, desviación)
- Total de errores por servidor
- Latencia promedio por servicio
- Conteo de eventos por nivel y tipo
- Peticiones totales por servicio
- Tráfico total (bytes) por servidor
- Eventos críticos
- Servidores con uso crítico de recursos
- Servicios con mayor tiempo de respuesta

## Flujo del sistema

```
┌─────────────────────────────────────────────────────────┐
│                    FLUJO KAFKA                           │
│                                                         │
│  Productor (Node.js) → Tópicos Kafka → Consumidor      │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│                    FLUJO SPARK                           │
│                                                         │
│  Datos (CSV/JSON/MySQL) → Spark → Resultados            │
└─────────────────────────────────────────────────────────┘
```

## Validacion del entorno

| Componente | Cómo verificar                                      |
|------------|-----------------------------------------------------|
| Kafka      | Listar tópicos con `kafka-topics.sh --list`         |
| Spark UI   | Acceder a `http://localhost:8080`                   |
| MySQL      | Consultar `SELECT COUNT(*)` en la tabla de logs     |
| Productor  | Ejecutar `npm run producer` y ver mensajes enviados |
| Consumidor | Ejecutar `npm run consumer` y ver mensajes leídos   |

## Modelo de datos

Cada registro contiene los siguientes campos:

| Campo                  | Descripción                          |
|------------------------|--------------------------------------|
| id_log                 | Identificador único                  |
| timestamp_evento       | Fecha y hora del evento              |
| servidor               | Nombre del servidor                  |
| ip_servidor            | Dirección IP                         |
| servicio               | Servicio que generó el evento        |
| tipo_evento            | Tipo (request, error, auth, etc.)    |
| nivel                  | INFO, WARNING, ERROR, CRITICAL       |
| codigo_estado          | Código HTTP                          |
| endpoint               | Ruta del servicio                    |
| usuario                | Usuario asociado                     |
| ciudad                 | Ciudad de origen                     |
| tiempo_respuesta_ms    | Tiempo de respuesta en ms            |
| uso_cpu_porcentaje     | Uso de CPU (%)                       |
| uso_ram_porcentaje     | Uso de RAM (%)                       |
| uso_disco_porcentaje   | Uso de disco (%)                     |
| bytes_entrada          | Bytes recibidos                      |
| bytes_salida           | Bytes enviados                       |
| peticiones_por_minuto  | Peticiones por minuto                |
| conexiones_activas     | Conexiones activas                   |
| errores_minuto         | Errores por minuto                   |
| latencia_red_ms        | Latencia de red en ms                |
| temperatura_cpu        | Temperatura del CPU                  |
| mensaje                | Mensaje descriptivo del evento       |

## Proximos pasos

1. Migrar el entorno a las tres máquinas físicas
2. Configurar replicación real (factor 3)
3. Probar tolerancia a fallos y distribución de carga
4. Documentar resultados y comparaciones entre fuentes

## Documentacion adicional

- [docs/alcance.md](docs/alcance.md) — Alcance del sistema
- [docs/arquitectura.md](docs/arquitectura.md) — Arquitectura técnica
- [docs/pruebas.md](docs/pruebas.md) — Pruebas realizadas
- [docs/resultados.md](docs/resultados.md) — Resultados obtenidos
