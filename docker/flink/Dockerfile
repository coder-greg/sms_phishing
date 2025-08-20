# syntax=docker/dockerfile:1.4
FROM flink:1.17

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

# Copy job jars into image (populated by scripts/build_and_run.sh into dist-jars/)
COPY dist-jars/ /opt/flink/jobs/

# Add wrapper entrypoint to auto-submit jars on JM start
COPY docker/flink/with-submit.sh /opt/flink/bin/with-submit.sh
RUN chmod +x /opt/flink/bin/with-submit.sh

USER flink

# Delegate commands like "jobmanager"/"taskmanager" to original entrypoint,
# but when role=jobmanager, submit all jars from /opt/flink/jobs after REST is up.
ENTRYPOINT ["/opt/flink/bin/with-submit.sh"]