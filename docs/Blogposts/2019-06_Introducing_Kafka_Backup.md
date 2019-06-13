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

Here, we describe the approaches very briefly. You can find more
Information in the
[Documentation](https://github.com/azapps/kafka-backup/blob/master/docs/Comparing_Kafka_Backup_Solutions.md)

## File System Snapshots

This was the easiest and most reliable way to backup data and consumer
offsets from Kafka. The procedure basically shuts down one broker
after another and performs a file system snapshot which is stored on
another (cold) disk.

## Using Mirror Maker 2 to backup data to another Cluster

The traditional Mirror Maker has many issues as discussed in
[KIP-382](https://cwiki.apache.org/confluence/display/KAFKA/KIP-382%3A+MirrorMaker+2.0). Mirror
Maker 2 approaches many of them and can be used to back up data from
one cluster to another.

Mirror Maker 2 is also (as kafka-backup) based on Kafka connect and
copies also consumer offsets.

## `kafka-connect-s3`

kafka-connect-s3 is a popular Kafka Connect Connector to mirror the
data from topics to Amazon S3 (or compatible other services like
Minio). Zalando describes a setup in their article [Surviving Data
Loss](https://jobs.zalando.com/tech/blog/backing-up-kafka-zookeeper/)

# Future Work

I think that a Backup strategy should be an essential part of any data
store. I aim for getting more feedback from the Kafka community and to
let Kafka Backup become a standardized piece of the Kafka Ecosystem.
