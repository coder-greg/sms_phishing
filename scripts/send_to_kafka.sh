#!/usr/bin/env bash
set -euo pipefail


TOPIC="sms-in"
KAFKA_CONTAINER="kafka"
BROKER="kafka:9092"

if [ $# -gt 0 ]; then
  echo "$*" | docker exec -i "$KAFKA_CONTAINER" kafka-console-producer --broker-list "$BROKER" --topic "$TOPIC"
else
  docker exec -i "$KAFKA_CONTAINER" kafka-console-producer --broker-list "$BROKER" --topic "$TOPIC"
fi