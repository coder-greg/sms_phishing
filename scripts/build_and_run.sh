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

echo "[1/4] Building Maven jobs under jobs/* ..."
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
echo "[2/4] Building custom Flink image with bundled jobs (local/sms-flink:latest) ..."
$COMPOSE build --pull flink-jobmanager

echo
echo "[3/4] Starting Kafka stack (Zookeeper, Kafka) and initializing topics ..."
$COMPOSE up -d zookeeper kafka
$COMPOSE up kafka-init

echo
echo "[4/4] Starting Flink cluster (JobManager, TaskManager) ..."
$COMPOSE up -d flink-jobmanager flink-taskmanager

cat << 'EOF'

Done.

Flink UI:
  localhost:8081

Useful commands:
  # List running jobs
  docker exec -it flink-jobmanager /opt/flink/bin/flink list -m localhost:8081

  # Tail JobManager logs
  docker logs -f flink-jobmanager

Notes:
  - Kafka bootstrap for jobs is provided via env var KAFKA_BOOTSTRAP_SERVERS (set to kafka:29092 in docker-compose).
  - To rebuild after code changes: re-run this script. It will:
      * Rebuild all job jars
      * Rebuild the custom Flink image (embedding the new jars)
      * Restart services as needed

EOF