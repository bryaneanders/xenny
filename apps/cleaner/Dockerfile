# Packages jars Maven has already built. From the repo root:
#   mvn -B package -pl apps/cleaner -am -DskipTests
#   docker build -f apps/cleaner/Dockerfile -t xenny-cleaner:dev .
FROM eclipse-temurin:25-jre
WORKDIR /app
# Dependencies first: they change less often than our code, so this layer is usually cached.
COPY apps/cleaner/target/lib/ lib/
COPY apps/cleaner/target/cleaner-*.jar cleaner.jar
USER 1000
# Avro only loads generated classes from trusted packages. RocksDB (the state store) loads a native library.
ENTRYPOINT ["java", "-Dorg.apache.avro.SERIALIZABLE_PACKAGES=xenny", "--enable-native-access=ALL-UNNAMED", \
            "-cp", "cleaner.jar:lib/*", "xenny.cleaner.Main"]
