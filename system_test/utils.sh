#!/bin/bash

kafka_start() {
  confluent local start kafka
}

kafka_stop() {
  confluent local stop
}

kafka_delete_data() {
  rm -r /tmp/confluent.*
}

kafka-topics() {
  "$CONFLUENT_HOME/bin/kafka-topics" $@
}
kafka-console-consumer() {
  "$CONFLUENT_HOME/bin/kafka-console-consumer" $@
}
kafka-consumer-groups() {
  "$CONFLUENT_HOME/bin/kafka-consumer-groups" $@
}

create_topic() {
  TOPIC=$1
  PARTITIONS=$2
  if [ -z "$PARTITIONS" ]; then
    echo "USAGE: $0 [TOPIC] [PARTITIONS]"
    return 255
  fi

  kafka-topics --create --bootstrap-server localhost:9092 --topic "$TOPIC" --partitions "$PARTITIONS" --replication-factor 1
}

gen_message() {
  PARTITION=$1
  NUM=$2
  if [ -z "$NUM" ]; then
    echo "USAGE: $0 [PARTITION] [NUM] (SIZE)"
    return 255
  fi
  SIZE=$3
  if [ -z "$SIZE" ]; then
    SIZE=7500 # 10k Bytes base64
  fi
  VALUE=$(dd if=/dev/urandom bs=$SIZE count=1 2>/dev/null | base64 -w0)
  CHECKSUM=$(echo "$VALUE" | md5sum | cut -d' ' -f1)
  KEY="part_${PARTITION}_num_${NUM}_${CHECKSUM}"
  echo "${KEY},${VALUE}"
}

gen_messages() {
  PARTITION=$1
  START_NUM=$2
  COUNT=$3
  if [ -z "$COUNT" ]; then
    echo "USAGE: $0 [PARTITION] [START_NUM] [COUNT] (SIZE)"
    return 255
  fi
  SIZE=$4
  for NUM in {$START_NUM..$((START_NUM + COUNT - 1))}; do
    if [ "0" -eq "$(((NUM - START_NUM) % 100))" ]; then
      echo -e -n "\rProduced $((NUM - START_NUM))/$COUNT messages" >/dev/stderr
    fi
    gen_message "$PARTITION" $NUM "$SIZE"
  done
  echo ""
}

produce_messages() {
  TOPIC=$1
  PARTITION=$2
  START_NUM=$3
  COUNT=$4
  if [ -z "$COUNT" ]; then
    echo "USAGE: $0 [TOPIC] [PARTITION] [START_NUM] [COUNT] (SIZE)"
    return 255
  fi
  SIZE=$5

  gen_messages "$PARTITION" "$START_NUM" "$COUNT" "$SIZE" | kafkacat -P -b localhost:9092 -t "$TOPIC" -p "$PARTITION" -K ","
}

verify_messages() {
  PREVIOUS_NUM="-1"
  while read -r MESSAGE; do
    if [ "0" -eq "$(((PREVIOUS_NUM + 1) % 10))" ]; then
      echo -e -n "\rVerified $((PREVIOUS_NUM + 1)) messages" >/dev/stderr
    fi
    KEY=$(echo "$MESSAGE" | awk '{print $1}')
    KEY_MATCH=$(echo "$KEY" | sed 's/part_\([0-9]*\)_num_\([0-9]*\)_\(.*\)$/\1\t\2\t\3/')
    KEY_PARTITION=$(echo "$KEY_MATCH" | awk '{print $1}')
    KEY_NUM=$(echo "$KEY_MATCH" | awk '{print $2}')
    KEY_CHECKSUM=$(echo "$KEY_MATCH" | awk '{print $3}')

    VALUE=$(echo "$MESSAGE" | awk '{print $2}')
    VALUE_CHECKSUM=$(echo "$VALUE" | md5sum | cut -d' ' -f1)

    if [ ! "$KEY_NUM" -eq "$((PREVIOUS_NUM + 1))" ]; then
      echo "Missing message. Previous message has num $PREVIOUS_NUM. This message has num $KEY_NUM"
      return 255
    fi
    PREVIOUS_NUM=$KEY_NUM

    if [ "$KEY_CHECKSUM" != "$VALUE_CHECKSUM" ]; then
      echo "Partition $KEY_PARTITION, Key $KEY_NUM, KChk $KEY_CHECKSUM, vlength ${#VALUE}, vchk: $VALUE_CHECKSUM"

      echo "Checksum mismatch: Checksum in key ($KEY_CHECKSUM) does not match Checksum of value ($VALUE_CHECKSUM)"
      return 255
    fi
  done
  echo -e "\rVerified $((PREVIOUS_NUM + 1)) messages"
}

consume_verify_messages() {
  TOPIC=$1
  PARTITION=$2
  COUNT=$3
  if [ -z "$COUNT" ]; then
    echo "USAGE: $0 [TOPIC] [PARTITION] [COUNT]"
    return 255
  fi

  kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --from-beginning --property print.key=true \
    --topic "$TOPIC" \
    --max-messages="$COUNT" \
    --partition="$PARTITION" 2>&/dev/null |
    verify_messages
}

consume_messages() {
  TOPIC=$1
  CONSUMER_GROUP=$2
  COUNT=$3
  if [ -z "$COUNT" ]; then
    echo "USAGE: $0 [TOPIC] [CONSUMER GROUP] [COUNT]"
    return 255
  fi

  MESSAGES=$(kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --from-beginning --property print.key=true \
    --topic "$TOPIC" \
    --max-messages "$COUNT" \
    --group "$CONSUMER_GROUP") # 2>/dev/null)
      echo "Consumed $(echo "$MESSAGES" | wc -l) messages"
}

kafka_connect_start() {
  confluent local start connect
}

kafka_connect_load_connector() {
  NAME=$1
  PROPS="$2"
  if [ -z "$PROPS" ]; then
    echo "USAGE: $0 [NAME] [PROPS FILE]"
    return 255
  fi
  cp "$PROPS" /tmp/connect.properties
  confluent local load "$NAME" -- -d /tmp/connect.properties
  rm /tmp/connect.properties
}

kafka_connect_unload_connector() {
  NAME=$1
  if [ -z "$NAME" ]; then
    echo "USAGE: $0 [NAME]"
    return 255
  fi
  confluent local unload "$NAME"
}

kafka_group_describe() {
  GROUP=$1
  if [ -z "$GROUP" ]; then
    echo "USAGE: $0 [GROUP]"
    return 255
  fi
  kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group "$GROUP"
}

burry_backup() {
  TARGET_DIR=$1
  if [ -z "$TARGET_DIR" ]; then
    echo "USAGE: $0 [TARGET_DIR]"
    return 255
  fi
  docker run --network=host -v "$TARGET_DIR":/data azapps/burry -e localhost:2181 -t local
}

burry_restore() {
  SOURCE_DIR=$1
  if [ -z "$SOURCE_DIR" ]; then
    echo "USAGE: $0 [SOURCE_DIR]"
    return 255
  fi
  SNAPSHOT=$(ls "$DATADIR"/burry | tail -n 1 | sed 's/.zip//')
  docker run --network=host -v "$SOURCE_DIR":/data azapps/burry --operation=restore --snapshot="$SNAPSHOT" -e localhost:2181 -t local
}
