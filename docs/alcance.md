# Alcance del sistema

## Nombre del proyecto

**Sistema distribuido para monitoreo y análisis de logs y métricas de servidores mediante Apache Kafka y Apache Spark**

## 1. Descripción general

El sistema propuesto tiene como finalidad simular, transmitir, almacenar y analizar logs y métricas generadas por servidores dentro de una arquitectura distribuida.

El proyecto utilizará Apache Kafka para la transmisión de eventos en tiempo real y Apache Spark para el procesamiento distribuido de grandes volúmenes de datos. La implementación se realizará primero en un entorno local contenerizado con Docker, y posteriormente se migrará a un entorno distribuido con tres máquinas físicas conectadas en red local.

El sistema permitirá demostrar conceptos fundamentales de procesamiento distribuido, tales como comunicación entre nodos, particionamiento, replicación, tolerancia a fallos, procesamiento paralelo, distribución de carga y análisis de datos masivos.

## 2. Objetivo general

Diseñar e implementar un sistema distribuido para la recolección, transmisión y análisis de logs y métricas de servidores utilizando Apache Kafka, Apache Spark y Docker, con el propósito de demostrar el funcionamiento de un entorno distribuido basado en tres nodos físicos.

## 3. Objetivos específicos

- Implementar un clúster de Apache Kafka sobre tres nodos físicos.
- Configurar Kafka en modo KRaft, donde cada nodo funcione como broker y controller.
- Crear al menos cinco tópicos de Kafka con particiones y factor de replicación.
- Desarrollar productores que simulen la generación de logs y métricas de servidores.
- Desarrollar consumidores que permitan leer los mensajes enviados a Kafka.
- Implementar un clúster de Apache Spark con un nodo Master y dos nodos Workers.
- Generar al menos 100,000 registros de prueba.
- Manejar datos en formato CSV, JSON y SQL.
- Procesar los datos mediante Spark para realizar análisis estadísticos.
- Comparar el procesamiento local contra el procesamiento distribuido.
- Realizar pruebas de caída de nodos para observar la tolerancia a fallos.
- Documentar la arquitectura, configuración, pruebas y resultados obtenidos.

## 4. Alcance funcional

El sistema incluirá las siguientes funcionalidades:

### 4.1 Generación de datos

Se desarrollará un generador de datos capaz de crear registros simulados de logs y métricas de servidores.

Los datos generados incluirán información como:

- Identificador del log.
- Fecha y hora del evento.
- Nombre del servidor.
- IP del servidor.
- Servicio que generó el evento.
- Tipo de evento.
- Nivel del log.
- Código de estado HTTP.
- Tiempo de respuesta.
- Uso de CPU.
- Uso de RAM.
- Uso de disco.
- Bytes de entrada.
- Bytes de salida.
- Peticiones por minuto.
- Conexiones activas.
- Errores por minuto.
- Latencia de red.
- Temperatura del CPU.
- Mensaje descriptivo.

Los datos se generarán en tres formatos:

- CSV.
- JSON.
- SQL.

### 4.2 Transmisión de eventos con Kafka

Kafka se utilizará para recibir y distribuir eventos de logs y métricas en tiempo real.

Se crearán al menos cinco tópicos:

- `metricas_recursos`
- `logs_http`
- `logs_errores`
- `metricas_red`
- `logs_seguridad`

Cada tópico podrá tener particiones y factor de replicación para demostrar la distribución y disponibilidad de los datos dentro del clúster.

### 4.3 Productores Kafka

Los productores simularán servidores o servicios que generan eventos continuamente.

Estos productores enviarán mensajes a Kafka en formato JSON. Cada mensaje representará un evento de monitoreo generado por un servidor.

Los productores podrán ser desarrollados en JavaScript con Node.js, utilizando una librería cliente de Kafka.

### 4.4 Consumidores Kafka

Los consumidores permitirán leer los mensajes desde los tópicos de Kafka.

Estos consumidores servirán para verificar que los eventos se transmiten correctamente y para comprobar el comportamiento del sistema cuando se ejecutan consumidores dentro de grupos de consumo.

### 4.5 Procesamiento con Spark

Apache Spark se utilizará para procesar los datos generados en formato CSV, JSON y SQL.

Los scripts de análisis permitirán obtener estadísticas como:

- Promedio de uso de CPU por servidor.
- Promedio de uso de RAM por servicio.
- Máximo uso de disco por servidor.
- Promedio de tiempo de respuesta por servicio.
- Cantidad de errores por servidor.
- Latencia promedio de red.
- Total de bytes enviados y recibidos.
- Cantidad de peticiones por minuto.
- Identificación de servidores críticos.
- Cálculo de máximos, mínimos, promedios y desviación estándar.

### 4.6 Contenerización con Docker

Docker se utilizará para ejecutar los servicios principales del proyecto en contenedores.

La implementación se dividirá en dos escenarios:

- Entorno local.
- Entorno distribuido en tres nodos físicos.

El entorno local permitirá realizar pruebas iniciales en una sola computadora. El entorno distribuido será la versión final del proyecto, donde los servicios se ejecutarán en tres máquinas físicas conectadas a una red local.

### 4.7 Base de datos SQL

Se incluirá una base de datos SQL para almacenar una versión estructurada de los logs y métricas generados.

La tabla principal será:

```sql
logs_metricas_servidores
```

Esta tabla contendrá los campos necesarios para representar los eventos generados por los servidores.

## 5. Alcance técnico

El proyecto contempla el uso de las siguientes tecnologías:

- Apache Kafka.
- Apache Spark.
- Docker.
- Docker Compose.
- JavaScript / Node.js.
- Python / PySpark.
- SQL.
- Red local con IPs fijas.
- Visual Studio Code.
- Git y GitHub.

## 6. Arquitectura incluida en el alcance

La arquitectura final estará compuesta por tres máquinas físicas.

### Nodo 1

- Kafka Broker + Controller 1.
- Spark Master.
- Cliente de prueba.
- IP fija sugerida: 192.168.1.101.

### Nodo 2

- Kafka Broker + Controller 2.
- Spark Worker 1.
- IP fija sugerida: 192.168.1.102.

### Nodo 3

- Kafka Broker + Controller 3.
- Spark Worker 2.
- IP fija sugerida: 192.168.1.103.

## 7. Pruebas incluidas

El proyecto incluirá pruebas para validar el funcionamiento de Kafka y Spark.

### Pruebas Kafka

- Creación del clúster de tres nodos.
- Creación de tópicos.
- Configuración de particiones.
- Configuración de factor de replicación.
- Envío de mensajes desde productores.
- Consumo de mensajes desde consumidores.
- Verificación de distribución de particiones.
- Desconexión controlada de un nodo.
- Verificación de continuidad operativa.
- Observación del comportamiento de líderes y réplicas.

### Pruebas Spark

- Inicio del Spark Master.
- Conexión de los Spark Workers.
- Lectura de datos CSV.
- Lectura de datos JSON.
- Lectura de datos SQL.
- Ejecución de análisis estadísticos.
- Procesamiento de al menos 100,000 registros.
- Comparación entre ejecución local y distribuida.
- Revisión de tiempos de ejecución.

## 8. Fuera del alcance

Para mantener el proyecto enfocado en Kafka, Spark y procesamiento distribuido, no se incluirán en la primera versión:

- Dashboard web en tiempo real.
- Sistema de autenticación de usuarios.
- Panel administrativo.
- Alertas por correo electrónico.
- Monitoreo real de servidores físicos.
- Sensores reales.
- Despliegue en la nube.
- Kubernetes.
- Machine Learning avanzado.
- Sistema de notificaciones externas.

Estas funcionalidades podrán considerarse como trabajo futuro.

## 9. Resultado esperado

Al finalizar el proyecto se espera contar con un sistema distribuido funcional capaz de:

- Generar datos simulados de logs y métricas de servidores.
- Transmitir eventos mediante Kafka.
- Consumir eventos desde Kafka.
- Procesar grandes volúmenes de datos mediante Spark.
- Ejecutarse primero en ambiente local y posteriormente en tres máquinas físicas.
- Demostrar particionamiento, replicación, procesamiento paralelo y tolerancia a fallos.
- Documentar claramente la arquitectura, configuración, pruebas y resultados.