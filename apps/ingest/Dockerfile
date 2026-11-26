# Packages jars Maven has already built. From the repo root:
#   mvn -B package -pl apps/ingest -am -DskipTests
#   docker build -f apps/ingest/Dockerfile -t xenny-ingest:dev .
FROM eclipse-temurin:25-jre
WORKDIR /app
# Dependencies first: they change less often than our code, so this layer is usually cached.
COPY apps/ingest/target/lib/ lib/
COPY apps/ingest/target/ingest-*.jar ingest.jar
USER 1000
# Avro only loads generated classes from trusted packages.
ENTRYPOINT ["java", "-Dorg.apache.avro.SERIALIZABLE_PACKAGES=xenny", "-cp", "ingest.jar:lib/*", "xenny.ingest.Main"]
