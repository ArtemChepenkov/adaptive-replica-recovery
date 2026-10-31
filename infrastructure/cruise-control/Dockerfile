# syntax=docker/dockerfile:1
FROM eclipse-temurin:17-jre-jammy AS jmx-exporter
RUN mkdir -p /opt/jmx-exporter \
    && curl --fail --show-error --location --retry 3 \
       https://github.com/prometheus/jmx_exporter/releases/download/1.4.0/jmx_prometheus_javaagent-1.4.0.jar \
       -o /opt/jmx-exporter/jmx_prometheus_javaagent.jar \
    && echo 'db1492e95a7ee95cd5e0a969875c0d4f0ef6413148d750351a41cc71d775f59a  /opt/jmx-exporter/jmx_prometheus_javaagent.jar' | sha256sum --check

FROM eclipse-temurin:17-jdk-jammy AS build
WORKDIR /workspace/cruise-control
COPY . /workspace/
# Keep the upstream build's Git-based version and commit detection working for
# both a standalone checkout and a submodule. Only the build copy is normalized.
RUN set -e; \
    if [ -f .git ]; then \
      git_dir=$(sed -n 's/^gitdir: //p' .git | tr -d '\r'); \
      test -n "$git_dir"; \
      cp -a "$git_dir" /tmp/cruise-control-git; \
      rm .git; \
      mv /tmp/cruise-control-git .git; \
      sed -i '/^[[:space:]]*worktree[[:space:]]*=/d' .git/config; \
    fi \
    && sed -i 's/\r$//' gradlew kafka-cruise-control-start.sh
RUN --mount=type=cache,target=/root/.gradle bash ./gradlew \
    :cruise-control:jar :cruise-control:copyDependantLibs \
    :cruise-control-metrics-reporter:jar --no-daemon --max-workers=2

FROM apache/kafka:4.0.0 AS kafka
COPY --from=build /workspace/cruise-control/cruise-control-metrics-reporter/build/libs/*.jar /opt/kafka/libs/
COPY --from=jmx-exporter /opt/jmx-exporter/ /opt/jmx-exporter/

FROM eclipse-temurin:17-jre-jammy AS runtime
WORKDIR /opt/cruise-control
COPY --from=jmx-exporter /opt/jmx-exporter/ /opt/jmx-exporter/
COPY --from=build /workspace/cruise-control/cruise-control/build/libs/ cruise-control/build/libs/
COPY --from=build /workspace/cruise-control/cruise-control/build/dependant-libs/ cruise-control/build/dependant-libs/
COPY --from=build /workspace/cruise-control/cruise-control-metrics-reporter/build/libs/ cruise-control-metrics-reporter/build/libs/
COPY --from=build /workspace/cruise-control/kafka-cruise-control-start.sh ./
COPY --from=build /workspace/cruise-control/config/log4j.properties config/log4j.properties
ENV KAFKA_HEAP_OPTS="-Xms256m -Xmx512m"
EXPOSE 9090
ENTRYPOINT ["bash", "kafka-cruise-control-start.sh", "config/cruisecontrol.properties"]
