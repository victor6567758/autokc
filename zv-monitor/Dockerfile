FROM eclipse-temurin:17-jre-jammy
WORKDIR /app

# version must match version.jmx-prometheus-javaagent in the parent pom.xml
ARG JMX_EXPORTER_VERSION=0.20.0
COPY --chmod=0644 --from=lib jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION}.jar /app/jmx_prometheus_javaagent.jar

COPY target/zv-monitor.jar /app/zv-monitor.jar

EXPOSE 5558
ENTRYPOINT ["java", \
  "-javaagent:/app/jmx_prometheus_javaagent.jar=5558:/app/jmx-exporter-config.yml", \
  "-jar", "/app/zv-monitor.jar"]

