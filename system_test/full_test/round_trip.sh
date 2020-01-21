#!/bin/bash
SCRIPT_DIR=$(dirname $0)
DATADIR=/tmp/kafka-backup/full_test/
source $SCRIPT_DIR/../utils.sh
NUM_MSG=100

# Uses confluent cli
#
# * Stop kafka if its running
kafka_stop
# * Delete all data
kafka_delete_data
rm -rf $DATADIR
mkdir -p $DATADIR
# * Start Kafka
kafka_start
# * Configure following topics:
#   * backup-test-1partition
#   * backup-test-3partitions
create_topic backup-test-1partition 1
create_topic backup-test-3partitions 3
# * Produce 3,00 messages, 10KiB each to each partition
#
# We need to chunk the production of messages as otherwise we cannot
# guarantee that the group consumer will evenly consume the partitions.
produce_messages backup-test-1partition 0 0 $((3 * NUM_MSG))
# backup-test-3partition
produce_messages backup-test-3partitions 0 0 $((3 * NUM_MSG))
produce_messages backup-test-3partitions 1 0 $((3 * NUM_MSG))
produce_messages backup-test-3partitions 2 0 $((3 * NUM_MSG))
# * Consume all messages with consumer-group `cg-3k`
consume_messages backup-test-1partition cg-3k $((3 * NUM_MSG))
consume_messages backup-test-3partitions cg-3k $((9 * NUM_MSG))
kafka_group_describe cg-3k
# * Produce 2 * NUM_MSG messages
produce_messages backup-test-1partition 0 $((3 * NUM_MSG)) $((2 * NUM_MSG))
# backup-test-3partition
produce_messages backup-test-3partitions 0 $((3 * NUM_MSG)) $((2 * NUM_MSG))
produce_messages backup-test-3partitions 1 $((3 * NUM_MSG)) $((2 * NUM_MSG))
produce_messages backup-test-3partitions 2 $((3 * NUM_MSG)) $((2 * NUM_MSG))
# * Consume all messages with consumer-group `cg-5k`
consume_messages backup-test-1partition cg-5k $((5 * NUM_MSG))
consume_messages backup-test-3partitions cg-5k $((15 * NUM_MSG))
# * Produce 100 more messages
produce_messages backup-test-1partition 0 $((5 * NUM_MSG)) $((1 * NUM_MSG))
# backup-test-3partition
produce_messages backup-test-3partitions 0 $((5 * NUM_MSG)) $((1 * NUM_MSG))
produce_messages backup-test-3partitions 1 $((5 * NUM_MSG)) $((1 * NUM_MSG))
produce_messages backup-test-3partitions 2 $((5 * NUM_MSG)) $((1 * NUM_MSG))
# * Start Kafka Connect distributed
kafka_connect_start
# * Configure backup-sink:
#   * segment size: 10MiB
#   * topics.regex: backup-test-*
sleep 10
kafka_connect_load_connector backup-sink "$SCRIPT_DIR/connect-backup-sink.properties"
sleep 10
# * Create another topic:
#   * backup-test-10partitions

create_topic backup-test-10partitions 10
# * Produce 1,00 messages as above and consume 500 messages as above
for i in {0..9} ; do
    produce_messages backup-test-10partitions $i 0 $((5 * NUM_MSG))
done
# To force segmentation rolling
produce_messages backup-test-1partition 0 $((6 * NUM_MSG)) $((15 * NUM_MSG))
# Consume some messages
# * Wait a few minutes
sleep $((60*5))
# * Stop Kafka
kafka_stop
# * Delete all data
kafka_delete_data
# * Start Kafka
kafka_start
# * Create all 3 topics as above (we are not testing zookeeper backup!)
create_topic backup-test-1partition 1
create_topic backup-test-3partitions 3
create_topic backup-test-10partitions 10
# * Start Kafka Connect distributed
kafka_connect_start
# * Configure backup-source
kafka_connect_load_connector backup-source $SCRIPT_DIR/connect-backup-source.properties
# * Wait for restore to finish
sleep $((60*15))
kafka_connect_unload_connector backup-source
# * Read all messages and check that they are the same as the ones that were written.
# * Subscribe to Kafka using the consumer groups as above (`cg-5k` and `cg-3k`) and check whether they are at the correct position

consume_verify_messages backup-test-1partition 0 $((21 * NUM_MSG))
for i in {0..2} ; do
    consume_verify_messages backup-test-3partitions $i $((6 * NUM_MSG))
done
for i in {0..9} ; do
    consume_verify_messages backup-test-10partitions $i $((5 * NUM_MSG))
done
