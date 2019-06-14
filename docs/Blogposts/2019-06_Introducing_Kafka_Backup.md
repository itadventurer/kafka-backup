---
title: Introducing Kafka Backup
subtitle: The missing Backup solution for Kafka
documentclass: scrartcl
author: Anatoly Zelenin
keywords:
 - Kafka
 - Backup
 - Operations
date: 2019-06-10
language: en
---

I assume you are familiar with Apache Kafka. If not, stop reading here
and grab a book or two or google for a few introductory
pieces. Otherwise let us skip the introduction and get straight to the
topic: Why there is a need to back up Kafka and how to do so.

# The need for a Backup Solution for Kafka

Kafka is a highly distributed system and can be configured to provide
a high level of resilience on its own: Using a large replication
factor we can survive the loss of several brokers and when we stretch
our Kafka Cluster across multiple data centers (the latency should stay
below 30ms) we can even survive the loss of data centers! So why should
we care about backups if we can just increase the distribution of
Kafka?

## Replication does not replace backups

Replication handles many error cases but by far not all. What about
the case that there is a bug in Kafka that deletes old data? What
about a misconfiguration of the topic (are you sure, that your value
of retention.ms is a millisecond value?)? What about an admin that
accidentally deleted the whole Prod Cluster because they thought they
were on dev? What about security breaches? If an attacker gets access
to your Kafka Management interface, they can do whatever they like.

Of course, this does not matter too much if you are using Kafka to
distribute click-streams data for your analytics department and it is
tolerable to loose some data. But if you use Kafka as your "central
nervous system" for your company and you store your core business data
in Kafka you better think about a cold storage backup for your Kafka
Cluster.

## Do you really need additional Kafka Clusters?

If you do not use Kafka for "Big Data" applications you are probably
totally fine with just one smallish Kafka Cluster. Maybe your
applications run only in one data center and this is totally ok for
you. Then there is probably no need to set up and operate additional
Kafka Clusters. If your data center with all your applications shuts
down what use is it that your stretched high available Kafka Cluster
survived? You would be probably absolutely fine if you have just a
backup of all your data on an (offline) storage that you could replay
to restore operations.

# What is Kafka Backup?

Kafka Backup consists of two Connectors for Kafka Connect: One for
Backup (Implemented as a Sink Connector) and one for the restore of
data (Implemented as a Source Connector). There is nothing special
about the Connectors – instead of using a third party system like a
database or external service to write data to or read data from, the
Connectors write or read data to/from the local file system.

The **Sink Connector** connects to the Kafka Broker and continuously
fetches data from the specified topics (use a regex or a list of
topics) and writes it to disk. Similar as Kafka, Kafka Backup does not
interpret data in any way but writes it without modification as bytes
to files. To simplify the handling of the data, Kafka Backup splits
the data into segments (similar to Kafka) and creates indices to
access that data faster. You can mount directly your storage
solution to a directory and let Kafka Backup write to that directory
or you use a cron job to move that data to a storage system of your
choice. As Kafka Backup is an append-only system, incremental backups
are trivial. Similarly you can simply delete old data by deleting old
segments if they reached their time to live (In that case, simply
recreate the indices before restoring to minimize confusion).

Similarly, the **Source Connector** connects to the Kafka Broker and
imports the data stored in the files to Kafka. To reduce errors, you
need to explicitly define the list of the topics to restore. And
again, Kafka Connect handles many edge cases for us and it is not a
problem if the Connector is restarted (or crashes) during
restoration. It continues where it crashed. Additionally to the actual
data in the topic, the source Connector also restores the offsets of
the consumers. It is not enough to just copy the `__consumer_offsets`
topic because the actual offsets of the messages may have
changed. Kafka Backup handles this. As there is no way to gracefully
stop a Connector when its finished, the restore Connector will just
Log every few seconds a message that it finished and will not continue
to push something to Kafka.

# Setup

Setting up Kafka Backup is very similar to setting up any other Kafka
Connect Connector. We describe this procedure in more detail in the
[documentation](https://github.com/azapps/kafka-backup/tree/master/docs).

First, you need to add the Kafka Backup `jar` to the Kafka Connect
Connector directory. This enables Kafka Connect to discover the
Connector. There is only one `jar` file for backup and restore.

The Kafka Backup Sink requires no special attention. Just define the
topics to backup and where to store it. To be able to sync the
consumer group offsets the Connector requires the connection details
to the Kafka Cluster.

The restore Connector is a bit tricky: The Kafka Backup Source
Connector requires an API which is currently (June 2019) under review
and which is also required by Mirror Maker 2. At the time of writing
it is planned for Kafka 2.4 (Current version is 2.2). Using the old
Kafka Connect results in the consumer offsets not being synced. This
may be acceptable in some cases but otherwise you need to compile
Kafka Connect 2.4 yourself or use our provided Docker image. You find
the details in the [documentation](https://github.com/azapps/kafka-backup/tree/master/docs).

Apart from that, the setup of the Kafka Backup Source Connector is
very similar to the Sink Connector: Define the topics to backup
explicitly and configure the source directory and cluster
credentials.

# Alternatives

At the time of writing, I believe that Kafka Backup is the only viable
solution if you really want to take a cold backup of your Kafka
data. Nevertheless there are several different approaches that other
people use to backup Kafka data and recover from disasters.

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
* Use a cron job to shut down the sink cluster (with one broker)
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
* Possible to use in downstream services that work only with S3 (e.g. Data
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
* Requires Kafka Connect binaries of Kafka 2.4


# Future Work

I think that a Backup strategy should be an essential part of any data
store. I aim for getting more feedback from the Kafka community and to
let Kafka Backup become a standardized piece of the Kafka Ecosystem.
