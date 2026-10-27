# syntax=docker/dockerfile:1
FROM eclipse-temurin:17-jdk-jammy AS build
WORKDIR /src
COPY cruise-control/ ./
RUN sed -i 's/\r$//' gradlew kafka-cruise-control-start.sh
RUN --mount=type=cache,target=/root/.gradle bash ./gradlew \
    :cruise-control:jar :cruise-control:copyDependantLibs \
    :cruise-control-metrics-reporter:jar --no-daemon --max-workers=2

FROM apache/kafka:4.0.0 AS kafka
COPY --from=build /src/cruise-control-metrics-reporter/build/libs/*.jar /opt/kafka/libs/

FROM eclipse-temurin:17-jre-jammy AS runtime
WORKDIR /opt/cruise-control
COPY --from=build /src/cruise-control/build/libs/ cruise-control/build/libs/
COPY --from=build /src/cruise-control/build/dependant-libs/ cruise-control/build/dependant-libs/
COPY --from=build /src/cruise-control-metrics-reporter/build/libs/ cruise-control-metrics-reporter/build/libs/
COPY --from=build /src/kafka-cruise-control-start.sh ./
COPY --from=build /src/config/log4j.properties config/log4j.properties
ENV KAFKA_HEAP_OPTS="-Xms256m -Xmx512m"
EXPOSE 9090
ENTRYPOINT ["bash", "kafka-cruise-control-start.sh", "config/cruisecontrol.properties"]
