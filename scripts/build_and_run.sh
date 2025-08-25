#!/usr/bin/env bash
set -euo pipefail

# Resolve repository root
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")"/.. >/dev/null 2>&1 && pwd)"

DIST="$ROOT_DIR/dist-jars"
echo "DIST = $DIST"

if docker compose version >/dev/null 2>&1; then
  COMPOSE="docker compose"
elif command -v docker-compose >/dev/null 2>&1; then
  COMPOSE="docker-compose"
else
  echo "ERROR: docker compose not found. Install Docker Compose v2 or docker-compose."
  exit 1
fi

echo "[1/3] Starting Kafka stack (Zookeeper, Kafka) and Redis, and initializing topics ..."
$COMPOSE up -d zookeeper kafka redis kafka-ui

echo "Waiting for Kafka broker to be available on localhost:9092 ..."
for i in {1..30}; do
  if docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
    echo "Kafka is up!"
    break
  else
    echo "Kafka not ready yet, retrying in 2s..."
    sleep 2
  fi
  if [ "$i" -eq 30 ]; then
    echo "ERROR: Kafka did not become ready in time."
    exit 1
  fi
done

echo "Waiting for Redis to be available on localhost:6379 ..."
for i in {1..30}; do
  if docker exec redis redis-cli ping | grep -q PONG; then
    echo "Redis is up!"
    break
  else
    echo "Redis not ready yet, retrying in 2s..."
    sleep 2
  fi
  if [ "$i" -eq 30 ]; then
    echo "ERROR: Redis did not become ready in time."
    exit 1
  fi
done

recreate_kafka_topic() {
  local topic="$1"
  if docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list | grep -q "^${topic}\$"; then
    echo "Topic ${topic} exists, deleting..."
    docker exec kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic "${topic}"
    sleep 2
  fi
  docker exec -it kafka kafka-topics --create --topic "${topic}" --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 || true
}

recreate_kafka_topic "sms-in"
recreate_kafka_topic "sms-out"
recreate_kafka_topic "sms-for-phishing"
recreate_kafka_topic "sms-scam"

echo "[2/3] Recreated kafka topics"
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list

echo
echo "[3/3] Starting Flink cluster (JobManager, TaskManager) ..."
$COMPOSE up -d --force-recreate --no-deps flink-jobmanager flink-taskmanager

cat << 'EOF'

Done.

Flink UI:
  http://localhost:8081

Kafka UI:
 http://localhost:8080 