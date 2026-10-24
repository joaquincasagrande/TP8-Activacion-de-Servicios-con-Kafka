#!/bin/sh
set -e

export PATH="$PATH:/opt/kafka/bin"
BOOTSTRAP_SERVER="${KAFKA_BROKER:-kafka:29092}"
RETENTION_MS=604800000 # 7 dias en milisegundos

echo "⏳ Esperando a que Kafka este listo en ${BOOTSTRAP_SERVER}..."
until kafka-topics.sh --bootstrap-server "${BOOTSTRAP_SERVER}" --list > /dev/null 2>&1; do
  echo "Kafka aun no responde, reintentando en 2 segundos..."
  sleep 2
done

echo "✅ Kafka disponible. Creando topicos de negocio y DLQ (3 particiones, retencion 7 dias)..."

TOPICS="
activation.requested
billing.events
provisioning.events
activation.events
activation.requested.dlq
billing.events.dlq
provisioning.events.dlq
activation.events.dlq
"

for TOPIC in $TOPICS; do
  echo " -> Creando topico: ${TOPIC}"
  kafka-topics.sh --bootstrap-server "${BOOTSTRAP_SERVER}" \
    --create \
    --if-not-exists \
    --topic "${TOPIC}" \
    --partitions 3 \
    --replication-factor 1 \
    --config retention.ms="${RETENTION_MS}"
done

echo "📋 Listado final de topicos en el cluster:"
kafka-topics.sh --bootstrap-server "${BOOTSTRAP_SERVER}" --list
echo "🎉 Inicializacion de topicos completada con exito."
