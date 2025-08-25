#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   ./scripts/consume_from_kafka.sh [topic]
#   # Defaults to sms-in if no topic is given

if [ $# -lt 1 ]; then
  echo "Usage: $0 <topic>"
  exit 1
fi

TOPIC="$1"
KAFKA_CONTAINER="kafka"
BROKER="kafka:9092"

docker exec -it "$KAFKA_CONTAINER" kafka-console-consumer --bootstrap-server "$BROKER" --topic "$TOPIC" --from-beginning --timeout-ms 5000