#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   ./scripts/send_to_kafka.sh "your message"
#   echo "your message" | ./scripts/send_to_kafka.sh
#   ./scripts/send_to_kafka.sh < file_with_messages.txt

TOPIC="sms-in"
KAFKA_CONTAINER="kafka"
BROKER="kafka:9092"

if [ $# -gt 0 ]; then
  # Send argument as message
  echo "$*" | docker exec -i "$KAFKA_CONTAINER" kafka-console-producer --broker-list "$BROKER" --topic "$TOPIC"
else
  # Read from stdin
  docker exec -i "$KAFKA_CONTAINER" kafka-console-producer --broker-list "$BROKER" --topic "$TOPIC"
fi