#!/bin/bash
SCRIPT_DIR=$(dirname $0)
DATADIR=/tmp/kafka-backup/001_simple_1partition_test/
source $SCRIPT_DIR/../utils.sh

# Uses confluent cli
#
# * Stop kafka if its running
kafka_stop
# * Delete all data
kafka_delete_data
rm -rf $DATADIR
mkdir -p $DATADIR


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

# 1 empty partition, one full
create_topic backup-test-empty 3
produce_messages backup-test-empty 0 0 300



########################## Backup
# * Start Kafka Connect distributed
kafka_connect_start
# * Configure backup-sink:
#   * segment size: 10MiB
#   * topics.regex: backup-test-1partition
kafka_connect_load_connector 001_simple_1partition_test_sink "$SCRIPT_DIR/connect-backup-sink.properties"
# * Wait a few minutes
sleep $((60*5))


########################## Destroy & Restore Cluster
# * Stop Kafka
kafka_stop
# * Delete all data
kafka_delete_data
# * Start Kafka
kafka_start
# * Create all 3 topics as above (we are not testing zookeeper backup!)
create_topic backup-test-1partition 1
create_topic backup-test-empty 3


########################## Restore topic
# * Start Kafka Connect distributed
kafka_connect_start
# * Configure backup-source
kafka_connect_load_connector 001_simple_1partition_test_source $SCRIPT_DIR/connect-backup-source.properties
kafka_connect_unload_connector 001_simple_1partition_test_source

consume_verify_messages backup-test-1partition 0 300
