# Build and Run

## Build Kafka with the API Changes required for `kafka-backup` restore

## Build `kafka-backup`

You should be able to build this with `./gradlew shadowJar`. Once the jar is generated in `build/libs`, include it in `CLASSPATH` (e.g., `export CLASSPATH=.:$CLASSPATH:/fullpath/to/kafka-backup-jar` )

Run: `connect-standalone example-connect-worker.properties
example-kafka-backup-sink.properties`
