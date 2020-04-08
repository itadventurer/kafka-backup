package de.azapps.kafkabackup.sink;

import de.azapps.kafkabackup.common.offset.DiskOffsetSink;
import de.azapps.kafkabackup.common.offset.OffsetSink;
import de.azapps.kafkabackup.common.offset.OffsetSinkScheduler;
import de.azapps.kafkabackup.common.offset.S3OffsetSink;
import de.azapps.kafkabackup.common.partition.PartitionException;
import de.azapps.kafkabackup.common.partition.PartitionWriter;
import de.azapps.kafkabackup.common.partition.cloud.S3PartitionWriter;
import de.azapps.kafkabackup.common.partition.disk.DiskPartitionWriter;
import de.azapps.kafkabackup.common.partition.disk.PartitionIndex;
import de.azapps.kafkabackup.common.record.Record;
import de.azapps.kafkabackup.common.segment.SegmentIndex;
import de.azapps.kafkabackup.storage.s3.AwsS3Service;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class BackupSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(BackupSinkTask.class);
    private Path targetDir;
    private String bucketName;
    private Map<TopicPartition, PartitionWriter> partitionWriters = new HashMap<>();
    private long maxSegmentSize;
    private OffsetSink offsetSink;
    private OffsetSinkScheduler offsetSinkScheduler;
    private StorageMode storageMode;
    private AwsS3Service awsS3Service;

    @Override
    public String version() {
        return "0.1";
    }

    @Override
    public void start(Map<String, String> props) {
        log.info("Log: Starting BackupSinkTask. Props: {}", props);

        BackupSinkConfig config = new BackupSinkConfig(props);
        AdminClient adminClient = AdminClient.create(config.adminConfig());

        storageMode = config.storageMode();

        switch (config.storageMode()) {
            case S3:
                awsS3Service = new AwsS3Service(config.region(), config.endpoint(), config.pathStyleAccessEnabled());
                bucketName = config.bucketName();
                offsetSink = new S3OffsetSink(adminClient, config.consumerGroupsSyncMaxAgeMs(), awsS3Service, bucketName);
                break;
            case DISK:
                targetDir = Paths.get(config.targetDir());
                maxSegmentSize = config.maxSegmentSize();
                createDirectories(Paths.get(config.targetDir()));
                offsetSink = new DiskOffsetSink(adminClient, config.consumerGroupsSyncMaxAgeMs(), targetDir);
                break;
            default:
                throw new RuntimeException(String.format("Invalid Storage Mode %s. Supported values are %s or %s", config.storageMode(), StorageMode.DISK, StorageMode.S3));
        }
        offsetSinkScheduler = new OffsetSinkScheduler(offsetSink);
        offsetSinkScheduler.start(config.consumerOffsetSyncIntervalMs());
        log.debug("Initialized BackupSinkTask");
    }

    private void createDirectories(Path targetDir) {
        try {
            Files.createDirectories(targetDir);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        try {
            records.forEach(sinkRecord -> {
                log.info("SinkRecord: {}", sinkRecord);
                TopicPartition topicPartition = new TopicPartition(sinkRecord.topic(), sinkRecord.kafkaPartition());
                PartitionWriter partition = partitionWriters.get(topicPartition);
                Record record = Record.fromSinkRecord(sinkRecord);
                log.info("Record: {}", record);
                partition.append(record);
                if (sinkRecord.kafkaOffset() % 100 == 0) {
                    log.debug("Backed up Topic {}, Partition {}, up to offset {}", sinkRecord.topic(),
                            sinkRecord.kafkaPartition(), sinkRecord.kafkaOffset());
                }
            });
        } catch (PartitionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void open(Collection<TopicPartition> partitions) {
        super.open(partitions);
        try {
            for (TopicPartition topicPartition : partitions) {
                PartitionWriter partitionWriter;
                switch (storageMode) {
                    case DISK:
                        partitionWriter = openDiskPartition(topicPartition);
                        break;
                    case S3:
                        partitionWriter = openS3Partition(topicPartition);
                        break;
                    default:
                        throw new RuntimeException(String.format("Invalid Storage Mode. Supported values are %s or %s", StorageMode.DISK, StorageMode.S3));
                }
                this.partitionWriters.put(topicPartition, partitionWriter);
            }
            if (partitions.isEmpty()) {
                log.info("No partitions assigned to BackupSinkTask");
            }
            offsetSink.setPartitions(partitionWriters.keySet());
        } catch (IOException | SegmentIndex.IndexException | PartitionIndex.IndexException e) {
            throw new RuntimeException(e);
        }
    }

    private PartitionWriter openDiskPartition(TopicPartition tp) throws IOException, SegmentIndex.IndexException, PartitionIndex.IndexException {
        Path topicDir = Paths.get(targetDir.toString(), tp.topic());
        Files.createDirectories(topicDir);
        DiskPartitionWriter partitionWriter = new DiskPartitionWriter(tp.topic(), tp.partition(), topicDir, maxSegmentSize);
        long lastWrittenOffset = partitionWriter.lastWrittenOffset();
        // In the DISK storage mode, we track our own offsets in the target data.
        // Note that we must *always* request that we seek to an offset here. Currently the
        // framework will still commit Kafka offsets even though we track our own (see KAFKA-3462),
        // which can result in accidentally using that offset if one was committed but no files
        // were written to disk. To protect against this, even if we
        // just want to start at offset 0 or reset to the earliest offset, we specify that
        // explicitly to forcibly override any committed offsets.
        if (lastWrittenOffset > 0) {
            context.offset(tp, lastWrittenOffset + 1);
            log.debug("Initialized Topic {}, Partition {}. Last written offset: {}"
                    , tp.topic(), tp.partition(), lastWrittenOffset);
        } else {
            // The offset was not found, so rather than forcibly set the offset to 0 we let the
            // consumer decide where to start based upon standard consumer offsets (if available)
            // or the consumer's `auto.offset.reset` configuration
            log.info("Resetting offset for {} based upon existing consumer group offsets or, if "
                    + "there are none, the consumer's 'auto.offset.reset' value.", tp);
        }
        return partitionWriter;
    }

    private PartitionWriter openS3Partition(TopicPartition tp) {
        return new S3PartitionWriter(awsS3Service, bucketName, tp);
        // In the S3 storage mode, we track offsets in the source kafka cluster (due to eventual consistency)
        // See approach described in https://www.confluent.io/blog/apache-kafka-to-amazon-s3-exactly-once/
        // So no call to context.offset here.
    }

    public void close(Collection<TopicPartition> partitions) {
        super.close(partitions);

        for (TopicPartition topicPartition : partitions) {
            PartitionWriter partitionWriter = partitionWriters.get(topicPartition);
            partitionWriter.close();
            partitionWriters.remove(topicPartition);
            log.debug("Closed BackupSinkTask for Topic {}, Partition {}"
                    , topicPartition.topic(), topicPartition.partition());
        }
        offsetSink.setPartitions(partitionWriters.keySet());
    }

    @Override
    public void stop() {
        // TODO: if an exception is thrown during start(), stop will be called before these are initialized. Need to to null check!
        partitionWriters.values().forEach(PartitionWriter::close);
        offsetSinkScheduler.stop();
        offsetSink.close();
        log.info("Stopped BackupSinkTask");
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        for (PartitionWriter partitionWriter : partitionWriters.values()) {
            partitionWriter.flush();
            log.debug("Flushed Topic {}, Partition {}"
                    , partitionWriter.topic(), partitionWriter.partition());
        }
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> preCommit(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        switch (storageMode) {
            case DISK:
                // Flush to disk and then return null to prevent offsets from being committed in source cluster
                flush(currentOffsets);
                return null;
            case S3:
                // Only commit offsets that where successfully written to S3
                Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
                // Note: we must iterate over all assigned partitions here,
                // since time-based rotation might have kicked in for partitions not present in last call to put().
                for (TopicPartition tp : partitionWriters.keySet()) {
                    Long offset = partitionWriters.get(tp).getLastCommittableOffset(); // this is already last written + 1
                    if (offset != null) {
                        log.trace("Mark {} : {} as ready to commit.", tp, offset);
                        offsetsToCommit.put(tp, new OffsetAndMetadata(offset));
                    }
                }
                return offsetsToCommit;
            default:
                // This shouldn't happen, but still:
                flush(currentOffsets);
                return currentOffsets;
        }
    }
}
