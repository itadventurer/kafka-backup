#!/bin/bash
SCRIPT_DIR=$(dirname $0)
DATADIR=/tmp/kafka-backup/backup_with_burry
source $SCRIPT_DIR/../utils.sh

# Uses confluent cli
#
# * Stop kafka if its running
kafka_stop
# * Delete all data
kafka_delete_data
rm -rf $DATADIR
mkdir -p $DATADIR/{burry,topics}


########################## Generate Data
# * Start Kafka
kafka_start
# * Configure following topics:
#   * backup-test-1partition
create_topic backup-test-1partition 1
produce_messages backup-test-1partition 0 0 300
consume_messages backup-test-1partition cg-100 100
consume_messages backup-test-1partition cg-200 200
consume_messages backup-test-1partition cg-300 300


########################## Backup
# * Start Kafka Connect distributed
kafka_connect_start
# * Configure backup-sink:
#   * segment size: 10MiB
#   * topics.regex: backup-test-1partition
kafka_connect_load_connector 001_simple_1partition_test_sink "$SCRIPT_DIR/connect-backup-sink.properties"
# * Wait a few minutes
sleep $((60*5))

########################## Backup Zookeeper
burry_backup $DATADIR/burry

########################## Destroy & Restore Cluster
# * Stop Kafka
kafka_stop
# * Delete all data
kafka_delete_data
# * Start Kafka
kafka_start

########################## Restore Zookeeper
burry_restore $DATADIR/burry
# Restart Kafka
kafka_stop
kafka_start


########################## Restore topic
# * Start Kafka Connect distributed
kafka_connect_start
# * Configure backup-source
kafka_connect_load_connector 001_simple_1partition_test_source $SCRIPT_DIR/connect-backup-source.properties
sleep $((60*5))
kafka_connect_unload_connector 001_simple_1partition_test_source

consume_verify_messages backup-test-1partition 0 300
