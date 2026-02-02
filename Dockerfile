# syntax=docker/dockerfile:1

# ---------------------------------------------------------------------------
# Stage 1: Build
# ---------------------------------------------------------------------------
FROM sbtscala/scala-sbt:eclipse-temurin-17.0.10_7_1.9.8_2.13.12 AS build

WORKDIR /build

# Cache dependency resolution by copying build definitions first
COPY build.sbt ./
COPY project/build.properties project/plugins.sbt project/
COPY project/*.scala project/

# Resolve dependencies (cached unless build defs change)
RUN sbt update

# Copy source modules
COPY udps-core/ udps-core/
COPY udps-storage/ udps-storage/
COPY udps-query/ udps-query/
COPY udps-catalog/ udps-catalog/
COPY udps-governance/ udps-governance/
COPY udps-integration/ udps-integration/
COPY udps-api/ udps-api/

# Build the fat JAR via sbt-assembly on the api module
RUN sbt "api/assembly"

# ---------------------------------------------------------------------------
# Stage 2: Runtime
# ---------------------------------------------------------------------------
FROM eclipse-temurin:17-jre-alpine AS runtime

LABEL org.opencontainers.image.title="udps" \
      org.opencontainers.image.description="Unified Data Platform Service (UDPS) - gRPC data platform" \
      org.opencontainers.image.vendor="GBMM / Seri-SA" \
      org.opencontainers.image.source="https://github.com/seri-sa/udps" \
      org.opencontainers.image.licenses="Proprietary"

# Install wget for healthcheck (alpine base already has it via busybox, but
# ensure it is present) and add tini for proper PID 1 signal handling
RUN apk add --no-cache tini wget \
    && rm -rf /var/cache/apk/*

# Non-root user
RUN addgroup -g 1001 udps \
    && adduser -u 1001 -G udps -D -h /app udps

WORKDIR /app

# Copy the assembled fat JAR from the build stage
COPY --from=build --chown=udps:udps /build/udps-api/target/scala-2.13/udps-api-assembly-*.jar /app/udps.jar

# Ports:
#   50060  gRPC
#   8443   HTTPS
#   9090   Metrics (Prometheus)
#   8081   Health
EXPOSE 50060 8443 9090 8081

# Switch to non-root user
USER udps:udps

# JVM tuning for containers
ENV JAVA_OPTS="-XX:+UseContainerSupport \
  -XX:MaxRAMPercentage=75.0 \
  -XX:InitialRAMPercentage=50.0 \
  -XX:+UseG1GC \
  -XX:+HeapDumpOnOutOfMemoryError \
  -XX:HeapDumpPath=/app/heapdump.hprof \
  -Djava.security.egd=file:/dev/./urandom \
  -Dfile.encoding=UTF-8"

HEALTHCHECK --interval=15s --timeout=5s --start-period=60s --retries=3 \
  CMD wget --quiet --tries=1 --spider http://localhost:8081/health || exit 1

ENTRYPOINT ["tini", "--"]
CMD ["sh", "-c", "exec java ${JAVA_OPTS} -jar /app/udps.jar"]
