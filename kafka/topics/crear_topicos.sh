#!/bin/bash

BOOTSTRAP_SERVER="localhost:9092"

TOPICS=(
  "metricas_recursos"
  "logs_http"
  "logs_errores"
  "metricas_red"
  "logs_seguridad"
)

echo "Creando tópicos de Kafka..."

for TOPIC in "${TOPICS[@]}"
do
  echo "Creando tópico: $TOPIC"

  /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server $BOOTSTRAP_SERVER \
    --create \
    --if-not-exists \
    --topic $TOPIC \
    --partitions 3 \
    --replication-factor 1
done

echo "Tópicos creados correctamente."

echo "Lista de tópicos:"
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server $BOOTSTRAP_SERVER \
  --list