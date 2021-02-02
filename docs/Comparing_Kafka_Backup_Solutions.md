# Comparing Kafka Backup Solutions

Basically there are three other ways to backup and restore data
from/to Kafka:

## File System Snapshots

This was the easiest and most reliable way to backup data and consumer
offsets from Kafka. The procedure basically shuts down one broker
after another and performs a file system snapshot which is stored on
another (cold) disk.

**Backup Procedure:**

* Repeat for each Kafka broker:
  1. Shut down the broker
  2. Take a snapshot of the Filesystem (optional)
  3. Copy the snapshot (or simply the files) to the backup storage
  4. Turn on the broker and wait until all partitions are in sync

**Restore Procedure:**

* Restore the snapshot for each broker
* Boot the brokers

**Advantages:**

* Uses native OS tools
* As this procedure needs to be done very often, the fear of shutting
  down a broker is minimized (especially for a team and environment
  with few Kafka expertise)
* Offsets are backed up and restored correctly
* Internal topics are backed up and restored correctly
* Compacted messages are deleted too
* Messages older than the retention time are deleted too
* Uses cold storage

**Disadvantages:**

* Each message is backed up `replication factor`-times. Even if it
  enough to store it without replication.
* Reduced availability as every broker needs to be turned of for a
  backup
* Incremental Backups are harder to achieve (e.g. due to partition
  rebalancing)
* **POTENTIAL DATA LOSS**: If the backup is performed during a
  partition rebalance (very likely when the backup takes a loooong
  time) the backup could miss a whole partition due to bad timing.


## Using Mirror Maker 2 to backup data to another Cluster

The traditional Mirror Maker has many issues as discussed in
[KIP-382](https://cwiki.apache.org/confluence/display/KAFKA/KIP-382%3A+MirrorMaker+2.0). Mirror
Maker 2 approaches many of them and can be used to back up data from
one cluster to another.

Mirror Maker 2 is also (as `kafka-backup`) based on Kafka connect and
copies consumer offsets too.

**Backup Procedure A+B (normal setup):**

* Set up the MM2 Connector that copies the data from the topic
  `[topic]` on the source cluster to the topic
  `[source-cluster-name].[topic]` on the cluster name.
* Mirror Maker 2 ensures that the messages are copied continously
  offsets are also copied to a separate topic

**Backup Procedure C (for consistent Snapshots):**

* Set up the sink (backup) cluster with one broker
* Set up the topics on the sink cluster with a replication factor of
  `1`
* Set up MM2 to copy data from the source cluster to the sink cluster
* Use a cronjob to shut down the sink cluster (with one broker)
  regularly and take a snapshot of the file system and store them on
  cold storage.

**Restore Procedure A (Use other cluster):**

* Use the offset sync topic to configure the consumer groups to
  consume from the correct offset.
* Setup the consumers to use the other cluster. Throw away the old
  one.
* Set up the clients to produce and consume from the new topics in the
  new cluster
* Set up a new Backup Cluster

**Restore Procedure B (Mirror data back):**

* Create a new Kafka Cluster
* Set up Mirror Maker 2 to copy the data to the new cluster
* Continue with procedure A

**Restore Procedure C (Mirror + Snapshot):**

* Use Procedure B or restore a new cluster from the file system
  snapshots
* Add more nodes accordingly
* Increase the replication factor to match the requirements
* Rebalance the partitions if needed
* Continue with procedure A

**Advantages:**

* Support for warm cluster fail-over (active-active, active-passive)
* Support for more advanced cluster topologies

**Disadvantages:**

* Requires a second Kafka Cluster
* Apart from `C` this is a warm backup and does not protect from
  bugs in Kafka or the underlying OS
* Requires custom implementation of the switch-over handling to the
  restored cluster
* Adds a lot of complexity in the setup

## `kafka-connect-s3`

`kafka-connect-s3` is a popular Kafka Connect connector to mirror the
data from topics to Amazon S3 (or compatible other services like
Minio). Zalando describes a setup in their article [Surviving Data
Loss](https://jobs.zalando.com/tech/blog/backing-up-kafka-zookeeper/)

**Backup procedure:**

* Set up the sink connector to use your S3 endpoint
* Set up another sink connector that backs up the `__consumer_offsets` topic.

**Restore procedure:**

* Set up the source connector to read the data from S3 into Kafka
* Manually extract the new offset for the consumers and manually
  identify which offset on the new Kafka cluster matches the old
  one. (This is not a trivial task – you would need to count the ACK'd
  messages from the beginning to find out the exact offset – and not
  forgetting about compacted and deleted messages)

**Advantages:**

* Cold backup (to S3)
* Possible to use in downstream services that work only with S3 (e.l. Data
  Warehouses)

**Disadvantages:**

* Supports only S3 (and compatible systems) as the storage backend
* No support for restoring consumer offsets (the method described
  above could be described as guesstimating and will not work in many
  edge cases)

## `kafka-backup`

`kafka-backup` is inspired heavily by the Mirror Maker 2 and
`kafka-connect-s3`. It consists of a sink and a source connector both
of which support the backup and restore of the topic data and also
consumer offsets.

**Backup Procedure:**

* Set up the Kafka Backup Sink connector
* Copy the backed up data to a backup storage of your choice
* See [GitHub](http://github.com/azapps/kafka-backup) for more
  information of how to back up your Kafka Cluster

**Restore Procedure**

* Set up the Kafka Backup Source connector
* Wait until it finished (see logs for information)
* Use the restored cluster

**Advantages:**

* Only solution which is able to restore topic data and also consumer
  offsets
* Only solution designed to take cold backups of Kafka
* Simple to do incremental backups

**Disadvantages:**

* See [GitHub](http://github.com/azapps/kafka-backup) for the current
  maturity status of the project
* Currently supports only the file system as the storage backend
* Requires Kafka Connect binaries of Kafka 2.3
