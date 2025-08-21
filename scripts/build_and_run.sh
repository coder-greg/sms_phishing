#!/usr/bin/env bash
set -euo pipefail

# Resolve repository root
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")"/.. >/dev/null 2>&1 && pwd)"

DIST="$ROOT_DIR/dist-jars"
echo "DIST = $DIST"

command -v mvn >/dev/null 2>&1 || { echo "ERROR: mvn not found. Install Maven."; exit 1; }
if docker compose version >/dev/null 2>&1; then
  COMPOSE="docker compose"
elif command -v docker-compose >/dev/null 2>&1; then
  COMPOSE="docker-compose"
else
  echo "ERROR: docker compose not found. Install Docker Compose v2 or docker-compose."
  exit 1
fi

echo "[1/5] Building Maven jobs under jobs/* ..."
mkdir -p "$DIST"
rm -rf "$DIST"/*

shopt -s nullglob
found_modules=0
for module in "$ROOT_DIR"/jobs/*; do
  if [[ -d "$module" && -f "$module/pom.xml" ]]; then
    found_modules=$((found_modules+1))
    name="$(basename "$module")"
    echo " - Building $name"
    (cd "$module" && mvn -DskipTests clean package)

    any=0
    for jar in "$module"/target/*.jar; do
      base="$(basename "$jar")"
      if [[ "$base" == original-* ]]; then
        continue
      fi
      any=1
      cp "$jar" "$DIST/${name}--$base"
      echo "   > Collected: ${name}--$base"
    done
    if [[ $any -eq 0 ]]; then
      echo "   ! WARN: No non-original jars found in $module/target"
    fi
  fi
done
if [[ $found_modules -eq 0 ]]; then
  echo "ERROR: No Maven modules found in jobs/* (missing pom.xml?)."
  exit 1
fi

echo
echo "[2/5] Building custom Flink image with bundled jobs (local/sms-flink:latest) ..."
$COMPOSE build --pull flink-jobmanager

echo
echo "[3/5] Starting Kafka stack (Zookeeper, Kafka) and Redis, and initializing topics ..."
$COMPOSE up -d zookeeper kafka redis

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

echo "[4/5] Recreated kafka topics"
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list

echo
echo "[5/5] Starting Flink cluster (JobManager, TaskManager) ..."
$COMPOSE up -d --force-recreate --no-deps flink-jobmanager flink-taskmanager

cat << 'EOF'

Done.

Flink UI:
  localhost:8081

Useful commands:
  # List running jobs
  docker exec -it flink-jobmanager /opt/flink/bin/flink list -m localhost:8081

  # Tail TaskManager logs
  docker logs -f flink-taskmanager