# Kafka Backup: Architecture

## Entities

We describe here only the entities themself. See
[below](#file-formats) for more information about the file formats.

### Partition

A Kafka Backup Partition represents exactly one Kafka Partition. As
partitions can grow very large in size, Kafka Backup splits the data
in the partitions to segments (similar to Kafka) and rotates the
segments after a certain (configurable) threshold size is met. For
faster access of the segments, there is exactly one index per
partition: It maps segment start offsets to their file names. This is
only a performance optimization. The index does not need to be backed
up – it is possible to restore the index given the segments. This
means, that it is also possible to delete old segments after a certain
TTL to free up space. Kafka Backup handles this gracefully. After
deleting old segments, the partition index needs to be regenerated.

### Segment

A segment is a Log of a number of records. When a segment reaches a
certain threshold, a new segment is created and no more data is
written to the old record anymore. As the segment is rotated after a
record is added to it, it is usual that a segment gets slightly bigger
than the threshold. In the extreme, each message is written to a new
segment.

For each segment, two files are created: A record file, that contains
a concatenated list of segments, and an index file that consist of a
mapping of offsets to positions in the file.

As it is impossible to guarantee exactly once delivery in a
distributed system, Kafka Backup supports idempotent writes. I.e., if
a message with the same offset arrives it is written to the same
position. Kafka Backup assumes that incoming messages with the same
offset are exactly the same (this is reasonable, because this is by
Kafkas design). Records are first written to the records file, then an
entry is appended to the index and only then the consumer offsets are
committed (asynchronously). This design guarantees, that committed
messages will appear exactly once in the segment.

### Record

Each segment is a concatenation of records. A record represents
exactly one message in Kafka. Please Note: For performance reasons
Kafka records can consist of multiple messages. We do not perform
these optimizations to improve simplicity of the file format.

Kafka Backup assumes that the messages consist of the informations as
defined in the Message Format of Kafka 0.11.0 (at the time of writing
(June 2019) this is the up to date message format). This means that
each record consists of following information:

* Offset
* Key
* Value
* Timestamp and type (no timestamp, create timestamp or log append time)
* Record Headers

### Offset

Additionally to the topic data, Kafka Backup saves also the consumer
offsets for each partition. We store for each Partition a simple JSON
file that contains a mapping from consumer groups to the current
committed offset of that group. Note, that it is not possible to map
the offsets during restoration 1:1. There are many edge cases that may
cause that the original message gets a completely different offset
after restoration:

* Retention policies
* Log compaction
* Errors during message production

All that errors can appear during production into the original cluster
but also during restore operations.

## Connectors

Kafka Backup consists of two Kafka Connect Connectors: A sink
connector responsible for the backup task and a source connector
responsible for the restoration.

The Kafka Connect Architecture distinguishes between Connectors and
Tasks: Tasks perform the actual work and the Connector acts as a
preparation and cleanup stage and configures the actual Tasks. For
performance reasons, Kafka Connect supports multiple tasks per
connector and distributes them across multiple Kafka Connect workers
if available.

Currently, Kafka Backup supports only one Task per Connector
configuration.

Both, The Sink and also the Backup Connector consist only of
boilerplate code. They just pass the configuration to the one task and
throw an Exception when the number of tasks is greater than 1.

### Sink Task

As usual, our Sink Task extends the Kafka Connect `SinkTask`. There
are two jobs, the sink task is responsible for: First, every time, Kafka
Connect delivers new Records to be backed up, the task writes it to
the appropriate partition files. The actual division into Segments is
abstracted away from the Sink Task: It just transforms the Kafka
Connect `SinkRecord` format to a Kafka Backup `Record` and `append`s
it to the appropriate partition.

Second, the Sink Task is also responsible for backing up the consumer
group offsets. Ideally this job would be scheduled independently of
the delivery of new messages from Kafka Connect. Currently the offsets
are synchronized every time, new records are pushed to Kafka
Connect. Note, that the sync of consumer offsets is not supported out
of the box in Kafka Connect. Thus we need to create our own
`AdminClient` that is responsible for fetching the offsets for all
consumer groups.

Note, that Kafka Connect supports the definition of topics to back up
via Regex and thus, it is possible that new partitions are added
during runtime. Kafka Backup supports this case.

### Source Task

In general, the source task is conceptually very similar to the Sink
task apart from the directory of the data flow.

Before performing the actual job, the source task collects all
required information about the partitions to be backed up and finds
the corresponding files on the file system.

The restore task, splits the incoming data in configurable batches and
performs the restore for each batch one after another. As there is no
way to gracefully shut down Kafka Connect from the inside, the Source
Task logs a completion message every few seconds after all data is
restored from the files.

To restore consumer offsets, the Source Task requires a new API
introduced by Mirror Maker 2: `commitRecord(SourceRecord record,
RecordMetadata metadata)`. This function is called for every record
that is written to Kafka. We check whether there is a consumer offset
for the original Kafka Offset. If this is the case, we identify the
offset of the written message using the `RecordMetadata` and commit
this offset for the appropriate consumer group.

## File Formats

Kafka Backup requires a directory to write the files to. It creates a
directory for each topic using the name of the topic. Each directory
contains following files:

| File Name                                                              | Number of files             | Description                               |
|------------------------------------------------------------------------|-----------------------------|-------------------------------------------|
| `consumer_offsets_partition_[partition-num]`                           | One per partition           | Offsets for the partition `partition-num` |
| `index_partition_[partition-num]`                                      | One per partition           | Partition Index                           |
| `segment_partition_[partition-num]_from_offset_[start-offset]_records` | Possible many per partition | Record File for the segment               |
| `segment_partition_[partition-num]_from_offset_[start-offset]_index`   | Possible many per partition | Index File for the segment                |

### Partition Index File

The partition Index File is a binary log file that concatenates
entries without any delimiter.

The file starts with the magic byte `0x01`. If the first byte is not
equal `0x01` then it is not compatible with the current version of
Kafka Backup.

Each entry is of the following form

| Length (in bits) | Name             | Data Type             | Comment                                                       |
|------------------|------------------|-----------------------|---------------------------------------------------------------|
| 32               | `filenameLength` | `int32`               | The length of the file name                                   |
| `filenameLength` | `filename`       | UTF8 formatted String | File name of the segment                                      |
| 64               | `startOffset`    | `int64`               | The offset of the first entry in the segment named `filename` |

### Segment

There are two files for each segment. Each of the files is a binary
log file that concatenates entries without any delimiter.

#### Record File

Kafka   Backup    records   are    very   similar   to    the   [Kafka
Record](https://kafka.apache.org/documentation/#recordbatch)      file
format  but  is  missing  some  optimization  techniques  to  increase
simplicity and reliability.

The file starts with the magic byte `0x01`. If the first byte is not
equal `0x01` then it is not compatible with the current version of
Kafka Backup.

Each entry is of the following form:

| Length (in bits) | Name            | Data Type         | Comment                                                                        |
|------------------|-----------------|-------------------|--------------------------------------------------------------------------------|
| 64               | `offset`        | `int64`           | The offset of the record in the source Kafka cluster                           |
| 32               | `timestampType` | `int32`           | Type of the timestamp: `-1`: no timestamp, `0`: CreateTime, `1`: LogAppendTime, `-2`: CreateTime but with Timestamp `null` (dirty workaround regarding https://github.com/itadventurer/kafka-backup/issues/92) |
| 0 or 64          | `timestamp`     | `optional<int64>` | Timestamp if exists                                                            |
| 32               | `keyLength`     | `int32`           | byte-length of the record key  `-1` if the key is `null`                       |
| `keyLength`      | `key`           | `byte[]`          | key (not interpreted in any way)                                               |
| 32               | `valueLength`   | `int32`           | byte-length of the record value. `-1` if the value is `null`                   |
| `valueLength`    | `value`         | `byte[]`          | value (not interpreted in any way)                                             |
| 32               | `headerSize`    | `int32`           | number of headers of the record                                                |
| calculated       | `headers`       | `Header[]`        | Concatenated headers of the record                                             |

##### Headers

Each header is of the following form:

| Length (in bits)    | Name                | Data Type | Comment                                                                               |
|---------------------|---------------------|-----------|---------------------------------------------------------------------------------------|
| 32                  | `headerKeyLength`   | `int32`   | byte-length of the header key. A key must not be `null`. (Althought, it can be empty) |
| `headerKeyLength`   | `headerKey`         | `byte[]`  | key (not interpreted in any way)                                                      |
| 32                  | `headerValueLength` | `int32`   | byte-length of the header value. `-1` if the value is `null`                          |
| `headerValueLength` | `headerValue`       | `byte[]`  | value (not interpreted in any way)                                                    |

#### Index File

The file starts with the magic byte `0x01`. If the first byte is not
equal `0x01` then it is not compatible with the current version of
Kafka Backup.

Each entry is of the following form

| Length (in bits) | Name                 | Data Type | Comment                                                                                                                  |
|------------------|----------------------|-----------|--------------------------------------------------------------------------------------------------------------------------|
| 64               | `offset`             | `int64`   | The offset of the record in the source Kafka cluster                                                                     |
| 64               | `recordFilePosition` | `int64`   | The start position in the record file of the record                                                                      |
| 64               | `recordByteLength`   | `int64`   | Length of the record in the record file. (`recordFilePosition + recordByteLength = recordFilePosition` of the new record |



### Offset

The offset file consists of a mapping from consumer groups to the
committed offset in the current partition. It is represented as a
simple JSON map where the map key is the consumer group and the value
is the offset.

Example:

```json
{
  "consumer-group1": 100,
  "consumer-group2": 200,
  "consumer-group3": 300,
  "consumer-group4": 300
}
```
