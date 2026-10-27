# Build from the repo root: docker build -f apps/ingest/Dockerfile -t xenny-ingest:dev .
# Compiles on the build machine's own architecture; the jars run on any platform.
FROM --platform=$BUILDPLATFORM maven:3.9-eclipse-temurin-25 AS build
WORKDIR /src
COPY . .
# The cache mount keeps downloaded dependencies between builds.
RUN --mount=type=cache,target=/root/.m2 mvn -B -q package -pl apps/ingest -am -DskipTests

FROM eclipse-temurin:25-jre
WORKDIR /app
COPY --from=build /src/apps/ingest/target/lib/ lib/
COPY --from=build /src/apps/ingest/target/ingest-*.jar ingest.jar
USER 1000
# Avro only loads generated classes from trusted packages.
ENTRYPOINT ["java", "-Dorg.apache.avro.SERIALIZABLE_PACKAGES=xenny", "-cp", "ingest.jar:lib/*", "xenny.ingest.Main"]
