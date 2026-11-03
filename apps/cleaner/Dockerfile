# Build from the repo root: docker build -f apps/cleaner/Dockerfile -t xenny-cleaner:dev .
# Compiles on the build machine's own architecture; the jars run on any platform.
FROM --platform=$BUILDPLATFORM maven:3.9-eclipse-temurin-25 AS build
WORKDIR /src
COPY . .
# The cache mount keeps downloaded dependencies between builds.
RUN --mount=type=cache,target=/root/.m2 mvn -B -q package -pl apps/cleaner -am -DskipTests

FROM eclipse-temurin:25-jre
WORKDIR /app
COPY --from=build /src/apps/cleaner/target/lib/ lib/
COPY --from=build /src/apps/cleaner/target/cleaner-*.jar cleaner.jar
USER 1000
# Avro only loads generated classes from trusted packages. RocksDB (the state store) loads a native library.
ENTRYPOINT ["java", "-Dorg.apache.avro.SERIALIZABLE_PACKAGES=xenny", "--enable-native-access=ALL-UNNAMED", \
            "-cp", "cleaner.jar:lib/*", "xenny.cleaner.Main"]
