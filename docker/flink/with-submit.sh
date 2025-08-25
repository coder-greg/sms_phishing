#!/usr/bin/env bash
set -euo pipefail

ORIG_ENTRYPOINT="/docker-entrypoint.sh"
ROLE="${1:-}"

submit_jobs() {
  echo "[with-submit] Waiting for JobManager REST API on http://localhost:8081 ..."
  for i in {1..120}; do
    if curl -sf http://localhost:8081/overview >/dev/null; then
      echo "[with-submit] JobManager REST is up."
      break
    fi
    sleep 2
  done

  if ! curl -sf http://localhost:8081/overview >/dev/null; then
    echo "[with-submit] ERROR: JobManager REST not available after timeout." >&2
    return 1
  fi

  shopt -s nullglob
  jars=(/opt/flink/jobs/*.jar)
  if [ ${#jars[@]} -eq 0 ]; then
    echo "[with-submit] No JARs found in /opt/flink/jobs; nothing to submit."
    return 0
  fi

  for jar in "${jars[@]}"; do
    echo "[with-submit] Submitting job: $jar (UserStateManagementJob)"
    /opt/flink/bin/flink run -d -m localhost:8081 -c pl.example.UserStateManagementJob "$jar" || {
      echo "[with-submit] WARNING: Submission failed for $jar (UserStateManagementJob)" >&2
    }
    echo "[with-submit] Submitting job: $jar (PhishingDetectionJob)"
    /opt/flink/bin/flink run -d -m localhost:8081 -c pl.example.PhishingDetectionJob "$jar" || {
      echo "[with-submit] WARNING: Submission failed for $jar (PhishingDetectionJob)" >&2
    }
  done
}

if [[ "$ROLE" == "jobmanager" ]]; then
  ( submit_jobs ) &
  exec "$ORIG_ENTRYPOINT" "$@"
else
  exec "$ORIG_ENTRYPOINT" "$@"
fi