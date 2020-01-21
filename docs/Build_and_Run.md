# Build and Run

## Build Kafka with the API Changes required for `kafka-backup` restore

Just clone Ryanne Dolan's Kafka repository containing the source code
for Mirror Maker 2 .

```sh
git clone https://github.com/ryannedolan/kafka.git
git checkout KIP-382
```



## Build `kafka-backup`

You should be able to build this with `./gradlew shadowJar`. Once the jar is generated in `build/libs`, include it in `CLASSPATH` (e.g., `export CLASSPATH=.:$CLASSPATH:/fullpath/to/kafka-backup-jar` )

Run: `connect-standalone example-connect-worker.properties
example-kafka-backup-sink.properties`


## Docker
