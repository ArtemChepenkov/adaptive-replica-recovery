#!/usr/bin/env bash
set -euo pipefail
sed -e "s/@BROKER_ID@/$BROKER_ID/g" \
    -e "s/@BROKER_RACK@/$BROKER_RACK/g" \
    -e "s/@PROCESS_ROLES@/$PROCESS_ROLES/g" \
    -e "s|@LISTENERS@|$LISTENERS|g" \
    -e "s/@EXTERNAL_PORT@/$EXTERNAL_PORT/g" \
    /etc/recovery/server.properties.template > /tmp/server.properties
/opt/kafka/bin/kafka-storage.sh format --ignore-formatted \
  --cluster-id "$CLUSTER_ID" --config /tmp/server.properties
# Attach only to the broker JVM; admin/format/healthcheck JVMs must not bind 9404.
KAFKA_OPTS="${KAFKA_OPTS:-} ${BROKER_JAVA_AGENT:-}" exec /opt/kafka/bin/kafka-server-start.sh /tmp/server.properties
