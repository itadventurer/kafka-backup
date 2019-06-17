# Kafka Backup: Tooling

**Before you start, you need to [Build and Run](./Build_and_Run.md)
Kafka Backup to get the `jar` file**

Before you go, you need to add the `kafka-backup.jar` to your
classpath:

```sh
export CLASSPATH="./path/to/kafka-backup.jar:$CLASSPATH"
```

If you are in the root directory of Kafka Backup, you can use:

```sh
export CLASSPATH="`pwd`/build/libs/kafka-backup.jar:$CLASSPATH"
```

## SegmentCLI

Basic usage:

```sh
java de.azapps.kafkabackup.cli.SegmentCLI
```

### List all records

```sh
java de.azapps.kafkabackup.cli.SegmentCLI \
  --list \
  --segment /path/to/segment_partition_123_from_offset_0000000123_records
```

### Show key and value of a specific offset in a segment

```sh
java de.azapps.kafkabackup.cli.SegmentCLI --show --segment /path/to/segment_partition_123_from_offset_0000000123_records --offset 597
```

### Formatting Options

Using the `--formatter` option you can customize how the keys and
values of the messages are formatted. The default is the
`RawFormatter` which prints the bytes as they are (i.e. as characters
to the console.

Implemented options:

* `de.azapps.kafkabackup.cli.formatters.RawFormatter`
* `de.azapps.kafkabackup.cli.formatters.UTF8Formatter`
* `de.azapps.kafkabackup.cli.formatters.Base64Formatter`

Example:

```sh
java de.azapps.kafkabackup.cli.SegmentCLI --list \
  --segment /path/to/segment_partition_123_from_offset_0000000123_records \
  --key-formatter de.azapps.kafkabackup.cli.formatters.Base64Formatter
```
## SegmentIndexCLI

The segment index is required for faster access to the records in the
segment file. It also simplifies the implementation of the idempotent
sink connector. The segment index does not need to be backed up, but
must exist before performing a restore.

### List Index entries

Displays information about the records referenced in the index.

```sh
java de.azapps.kafkabackup.cli.SegmentIndexCLI --list \
  --segment-index /path/to/segment_partition_123_from_offset_0000000123_records \
```

### Restore Index

Given a record file, restores the segment index for that file.

```sh
java de.azapps.kafkabackup.cli.SegmentIndexCLI --restore-index \
  --segment /path/to/segment_partition_123_from_offset_0000000123_records
```

### Restoring all Segment Indexes

```sh
export TOPICDIR="/path/to/topicdir/"
for f in "$TOPICDIR"/segment_partition_*_records ; do
  java de.azapps.kafkabackup.cli.SegmentIndexCLI --restore-index \
  --segment $f
done
```

## PartitionIndexCLI

The partition index contains the information about which offsets are
located in which segment. This file too, does not need to be backed up
but is required for restoration.

It is totally ok to delete old segments that are not needed
anymore. But it is crucial to restore the partition index afterwards.

### List Index entries

```sh
java de.azapps.kafkabackup.cli.PartitionIndexCLI --list \
  --partition-index /path/to/index_partition_123
```

### Restore Partition Index

```sh
java de.azapps.kafkabackup.cli.PartitionIndexCLI --restore \
  --partition 0 \
  --topic-dir /path/to/topicdir/
```

#### Restore Indexes for all Partitions

```sh
export NUM_PARTITIONS=9
export TOPICDIR="/path/to/topicdir/"
for i in {0..$(( $NUM_PARTITIONS - 1 ))} ; do
    java de.azapps.kafkabackup.cli.PartitionIndexCLI --restore --partition $i --topic-dir "$TOPICDIR"
done
```
