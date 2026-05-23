# Arquitectura del sistema

## 1. Nombre del proyecto

**Sistema distribuido para monitoreo y análisis de logs y métricas de servidores mediante Apache Kafka y Apache Spark**

## 2. Descripción de la arquitectura

La arquitectura del sistema está diseñada para simular un entorno distribuido de monitoreo de servidores.

El sistema estará compuesto por tres nodos físicos conectados mediante una red local. Cada nodo ejecutará servicios mediante contenedores Docker. Apache Kafka se utilizará para la transmisión de eventos en tiempo real, mientras que Apache Spark se utilizará para el procesamiento distribuido de los datos generados.

La implementación se dividirá en dos etapas:

1. Entorno local contenerizado.
2. Entorno distribuido en tres máquinas físicas.

El entorno local permitirá desarrollar y probar el sistema en una sola computadora. El entorno distribuido permitirá cumplir el objetivo principal del proyecto: demostrar comunicación entre nodos, tolerancia a fallos, replicación, particionamiento y procesamiento paralelo.

## 3. Arquitectura general

```text
                         RED LOCAL
                192.168.1.0/24 o red equivalente

┌──────────────────────────┐
│        NODO 1             │
│  IP: 192.168.1.101        │
│                          │
│  Kafka Broker 1           │
│  Kafka Controller 1       │
│  Spark Master             │
│  Productor de prueba      │
└─────────────┬────────────┘
              │
              │
┌─────────────┴────────────┐
│        NODO 2             │
│  IP: 192.168.1.102        │
│                          │
│  Kafka Broker 2           │
│  Kafka Controller 2       │
│  Spark Worker 1           │
│  Consumidor de prueba     │
└─────────────┬────────────┘
              │
              │
┌─────────────┴────────────┐
│        NODO 3             │
│  IP: 192.168.1.103        │
│                          │
│  Kafka Broker 3           │
│  Kafka Controller 3       │
│  Spark Worker 2           │
└──────────────────────────┘
```

## 4. Estrategia de generación de datos

El sistema utilizará datos simulados para representar logs y métricas de servidores.

La generación de datos se dividirá en dos partes:

1. Datos descriptivos generados con Faker.
2. Métricas numéricas generadas con rangos controlados.

Faker se utilizará para generar datos variables y realistas, como usuarios, ciudades, fechas e IPs. Sin embargo, las métricas de rendimiento no se generarán directamente con Faker, ya que deben mantener coherencia estadística.

Por ejemplo:

- Los eventos `INFO` tendrán valores normales de CPU, RAM, latencia y errores.
- Los eventos `WARNING` tendrán valores moderadamente altos.
- Los eventos `ERROR` tendrán valores altos.
- Los eventos `CRITICAL` tendrán valores críticos.

Esta estrategia permite que los análisis realizados con Spark tengan sentido, ya que los datos presentan patrones detectables.

## 5. Componentes principales

### 5.1 Apache Kafka

Apache Kafka será utilizado como plataforma de mensajería distribuida para recibir, almacenar y distribuir eventos generados por servidores simulados.

Kafka funcionará en un clúster de tres nodos físicos. Cada nodo tendrá funciones de broker y controller mediante el modo KRaft.

#### Responsabilidades de Kafka

- Recibir eventos desde productores.
- Almacenar mensajes en tópicos.
- Distribuir mensajes en particiones.
- Replicar datos entre nodos.
- Permitir el consumo de mensajes por consumidores.
- Mantener continuidad ante la caída de un nodo.

#### Nodos Kafka

- Nodo 1 → Kafka Broker 1 + Controller 1
- Nodo 2 → Kafka Broker 2 + Controller 2
- Nodo 3 → Kafka Broker 3 + Controller 3

### 5.2 Tópicos Kafka

Se crearán al menos cinco tópicos para separar los eventos por tipo.

- `metricas_recursos`
- `logs_http`
- `logs_errores`
- `metricas_red`
- `logs_seguridad`

#### Descripción de tópicos

| Tópico              | Descripción                                                                       |
| ------------------- | --------------------------------------------------------------------------------- |
| `metricas_recursos` | Eventos relacionados con CPU, RAM, disco y temperatura.                           |
| `logs_http`         | Eventos relacionados con peticiones HTTP, códigos de estado y tiempos de respuesta. |
| `logs_errores`      | Eventos relacionados con errores de servicios o fallos del sistema.               |
| `metricas_red`      | Eventos relacionados con latencia, tráfico de red y conexiones activas.           |
| `logs_seguridad`    | Eventos relacionados con accesos, autenticaciones fallidas e IPs sospechosas.     |

### 5.3 Productores Kafka

Los productores serán programas encargados de enviar mensajes a Kafka.

Cada productor simulará eventos generados por servidores o servicios. Los mensajes se enviarán en formato JSON hacia el tópico correspondiente.

Ejemplo de productor:

```text
kafka/producers/producer_logs.js
```

#### Flujo del productor

```text
Generador de evento
        ↓
Productor Kafka
        ↓
Tópico Kafka
        ↓
Particiones del tópico
```

### 5.4 Consumidores Kafka

Los consumidores serán programas encargados de leer mensajes desde Kafka.

Se utilizarán para verificar que los mensajes enviados por los productores llegan correctamente a los tópicos.

Ejemplo de consumidor:

```text
kafka/consumers/consumer_logs.js
```

#### Flujo del consumidor

```text
Tópico Kafka
        ↓
Consumidor Kafka
        ↓
Lectura y visualización del mensaje
```

### 5.5 Apache Spark

Apache Spark será utilizado para el procesamiento distribuido de los datos generados.

El clúster Spark estará compuesto por un nodo Master y dos nodos Workers.

- Nodo 1 → Spark Master
- Nodo 2 → Spark Worker 1
- Nodo 3 → Spark Worker 2

#### Responsabilidades de Spark

- Leer archivos CSV.
- Leer archivos JSON.
- Leer datos SQL.
- Procesar al menos 100,000 registros.
- Realizar consultas y agregaciones.
- Calcular estadísticas.
- Distribuir tareas entre los Workers.
- Comparar procesamiento local contra procesamiento distribuido.

### 5.6 Spark Master

El Spark Master será el nodo encargado de coordinar la ejecución de los trabajos.

Sus funciones principales son:

- Administrar los recursos del clúster.
- Recibir aplicaciones Spark.
- Distribuir tareas entre los Workers.
- Supervisar la ejecución de los trabajos.

### 5.7 Spark Workers

Los Spark Workers serán los nodos encargados de ejecutar las tareas asignadas por el Master.

Sus funciones principales son:

- Ejecutar tareas de procesamiento.
- Procesar particiones de datos.
- Reportar estado al Master.
- Participar en el procesamiento paralelo.

### 5.8 Base de datos SQL

La base de datos SQL almacenará una versión estructurada de los logs y métricas generados.

La tabla principal será:

```text
logs_metricas_servidores
```

La base de datos se utilizará como una de las fuentes de datos para Spark.

### 5.9 Datos CSV y JSON

Además de la base de datos SQL, se generarán archivos CSV y JSON.

Estos archivos se almacenarán en:

```text
data/raw/logs_metricas.csv
data/raw/logs_metricas.json
data/raw/logs_metricas.sql
```

Spark leerá estos archivos para ejecutar análisis distribuidos.

## 6. Modelo de datos

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

### Ejemplo de registro JSON

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

## 7. Flujo general del sistema

### Flujo de procesamiento histórico

```text
Script generador con Faker + métricas controladas
        ↓
Archivos CSV / JSON / SQL
        ↓
Apache Spark
        ↓
Procesamiento estadístico distribuido
        ↓
Resultados en spark/output/
```

### Flujo de eventos en tiempo real

```text
Script generador de eventos
        ↓
Productor Kafka
        ↓
Tópicos Kafka
        ↓
Particiones replicadas
        ↓
Consumidores Kafka
```

### Flujo completo

```text
Servidores simulados
        ↓
Logs y métricas
        ↓
Productor Kafka
        ↓
Apache Kafka
        ↓
Consumidor Kafka

Datos históricos
        ↓
CSV / JSON / SQL
        ↓
Apache Spark
        ↓
Análisis estadístico
        ↓
Resultados
```

## 8. Red del sistema

La red principal será una red local proporcionada por un módem, router o switch.

Cada máquina tendrá una IP fija o reservada.

| Nodo   | IP sugerida     | Rol Kafka              | Rol Spark |
| ------ | --------------- | ---------------------- | --------- |
| Nodo 1 | 192.168.1.101   | Broker + Controller 1  | Master    |
| Nodo 2 | 192.168.1.102   | Broker + Controller 2  | Worker 1  |
| Nodo 3 | 192.168.1.103   | Broker + Controller 3  | Worker 2  |

## 9. Puertos principales

| Servicio            | Puerto | Descripción                          |
| ------------------- | ------ | ------------------------------------ |
| Kafka Broker        | 9092   | Comunicación de clientes Kafka.      |
| Kafka Controller    | 9093   | Comunicación interna del modo KRaft. |
| Spark Master        | 7077   | Conexión de aplicaciones Spark.      |
| Spark Master Web UI | 8080   | Interfaz web del Master.             |
| Spark Worker Web UI | 8081   | Interfaz web del Worker.             |
| Base de datos SQL   | 3306   | Conexión a base de datos.            |

## 10. Entorno local

El entorno local se utilizará para desarrollar y probar el sistema en una sola máquina.

Ubicación:

```text
docker/local/docker-compose.yml
```

Este entorno permitirá probar:

- Kafka local.
- Spark local.
- Productores.
- Consumidores.
- Generador de datos.
- Scripts PySpark.
- Base de datos SQL.

## 11. Entorno distribuido

El entorno distribuido será la versión final del proyecto.

Ubicación:

```text
docker/cluster/nodo1/docker-compose.yml
docker/cluster/nodo2/docker-compose.yml
docker/cluster/nodo3/docker-compose.yml
```

Cada archivo tendrá la configuración correspondiente a su nodo físico.

## 12. Migración de local a distribuido

La migración se realizará de forma gradual.

### Etapa 1

Probar todos los componentes en una sola máquina.

### Etapa 2

Asignar IP fija a cada máquina física.

### Etapa 3

Configurar Kafka con las IPs reales de cada nodo.

### Etapa 4

Configurar Spark Master y Workers.

### Etapa 5

Verificar comunicación entre nodos.

### Etapa 6

Ejecutar pruebas de envío, consumo y procesamiento.

### Etapa 7

Realizar pruebas de caída de nodos.

## 13. Consideraciones importantes

Para que el sistema distribuido funcione correctamente se deben considerar los siguientes puntos:

- No usar `localhost` en configuraciones distribuidas.
- Usar IPs reales de la red local.
- Verificar que las máquinas puedan hacerse ping.
- Verificar que los puertos estén abiertos.
- Evitar cambios de IP mediante reserva DHCP o configuración manual.
- Revisar los logs de los contenedores.
- Documentar cada prueba realizada.
- Mantener separada la configuración local de la configuración distribuida.

## 14. Arquitectura esperada final

Al finalizar el proyecto, se espera contar con:

- Tres nodos físicos conectados en red local.
- Kafka funcionando como clúster de tres nodos en modo KRaft.
- Spark funcionando con un Master y dos Workers.
- Datos generados en CSV, JSON y SQL.
- Productores enviando eventos a Kafka.
- Consumidores leyendo eventos desde Kafka.
- Spark procesando al menos 100,000 registros.
- Pruebas documentadas de tolerancia a fallos.
- Resultados estadísticos generados por Spark.