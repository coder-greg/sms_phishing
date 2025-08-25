# syntax=docker/dockerfile:1.4
FROM flink:1.17

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

COPY dist-jars/ /opt/flink/jobs/

COPY docker/flink/with-submit.sh /opt/flink/bin/with-submit.sh
RUN chmod +x /opt/flink/bin/with-submit.sh

USER flink

ENTRYPOINT ["/opt/flink/bin/with-submit.sh"]