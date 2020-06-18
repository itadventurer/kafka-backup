# Kafka Backup: Tooling

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

## Completed segments processing

You may want to process completed segment files. Let's say you have your
`target.dir` backed up to cloud storage daily. So you don't need to keep all
the files locally then. To save some space you may delete completed segment
files. There is `bin/completed_segments.py` script for your convenience.

To get some information on segment files just call script with path to your
backup directory.

```sh
completed_segments.py /path/to/target_dir
```

To delete completed segments use `-d` option.
```sh
completed_segments.py -d /path/to/target_dir
```

You may keep last N completed segments by using `-k N` option.

If you need more complex processing you may just list completed segment files
and pass them for further processing. E.g. to keep last 2 segments and `shred`
the rest run the following command.

```sh
completed_segments.py -l -k 2 /path/to/target_dir | xargs shred -u
```
