#!/usr/bin/env bash
set -euo pipefail

TOPIC="sms-in"
KAFKA_CONTAINER="kafka"
BROKER="kafka:9092"


if [ $# -gt 0 ]; then
  echo "$*" | jq -c . | docker exec -i "$KAFKA_CONTAINER" kafka-console-producer --broker-list "$BROKER" --topic "$TOPIC"
else
  if [ -t 0 ]; then
    docker exec -i "$KAFKA_CONTAINER" kafka-console-producer --broker-list "$BROKER" --topic "$TOPIC"
  else
    jq -c . | docker exec -i "$KAFKA_CONTAINER" kafka-console-producer --broker-list "$BROKER" --topic "$TOPIC"
  fi
fi