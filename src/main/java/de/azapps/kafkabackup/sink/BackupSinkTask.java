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
import org.apache.kafka.common.errors.RetriableException;
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

    private static final Long MIN_TIMEOUT_MS = 200L;
    private static final Logger log = LoggerFactory.getLogger(BackupSinkTask.class);

    private Map<TopicPartition, PartitionWriter> partitionWriters = new HashMap<>();
    private OffsetSink offsetSink;
    private OffsetSinkScheduler offsetSinkScheduler;
    private StorageMode storageMode;
    // DISK-specific:
    private Path targetDir;
    private long maxSegmentSize;
    // S3-specific:
    private String bucketName;
    private AwsS3Service awsS3Service;
    private int maxBatchMessages;
    private long maxBatchTimeMs;
    private long timeoutMs = MIN_TIMEOUT_MS;

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
                maxBatchMessages = config.maxBatchMessages();
                maxBatchTimeMs = config.maxBatchTimeMs();
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
                TopicPartition topicPartition = new TopicPartition(sinkRecord.topic(), sinkRecord.kafkaPartition());
                PartitionWriter partition = partitionWriters.get(topicPartition);
                Record record = Record.fromSinkRecord(sinkRecord);
                partition.append(record);
            });
        } catch (PartitionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void open(Collection<TopicPartition> partitions) {
        super.open(partitions);
        if (partitions.isEmpty()) {
            log.info("No partitions assigned to BackupSinkTask");
        }
        for (TopicPartition topicPartition : partitions) {
            openPartition(topicPartition);
        }
        offsetSink.setPartitions(partitionWriters.keySet());
    }

    private void openPartition(TopicPartition tp) {
        PartitionWriter partitionWriter = newPartitionWriter(tp);
        partitionWriters.put(tp, partitionWriter);
        switch(storageMode) {
            case DISK:
                Long offset = partitionWriter.getLastCommittableOffset();
                // In the DISK storage mode, we track our own offsets in the target data.
                // Note that we must *always* request that we seek to an offset here. Currently the
                // framework will still commit Kafka offsets even though we track our own (see KAFKA-3462),
                // which can result in accidentally using that offset if one was committed but no files
                // were written to disk. To protect against this, even if we
                // just want to start at offset 0 or reset to the earliest offset, we specify that
                // explicitly to forcibly override any committed offsets.
                if (offset != null) {
                    context.offset(tp, offset);
                    log.debug("Initialized Topic {}, Partition {}. Offset {}", tp.topic(), tp.partition(), offset);
                } else {
                    // The offset was not found, so rather than forcibly set the offset to 0 we let the
                    // consumer decide where to start based upon standard consumer offsets (if available)
                    // or the consumer's `auto.offset.reset` configuration
                    log.info("Resetting offset for {} based upon existing consumer group offsets or, if "
                            + "there are none, the consumer's 'auto.offset.reset' value.", tp);
                }
            default:
                // In the S3 storage mode, we let kafka take care of offsets. So no call to context.offset here.
        }
    }

    private PartitionWriter newPartitionWriter(TopicPartition tp) {
        switch (storageMode) {
            case DISK:
                try {
                    Path topicDir = Paths.get(targetDir.toString(), tp.topic());
                    Files.createDirectories(topicDir);
                    return new DiskPartitionWriter(tp.topic(), tp.partition(), topicDir, maxSegmentSize);
                } catch (IOException | SegmentIndex.IndexException | PartitionIndex.IndexException e) {
                    throw new RuntimeException(e);
                }
            case S3:
                return new S3PartitionWriter(awsS3Service, bucketName, tp, maxBatchMessages, maxBatchTimeMs);
            default:
                throw new RuntimeException(String.format("Invalid Storage Mode. Supported values are %s or %s", StorageMode.DISK, StorageMode.S3));
        }
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
        flush();
    }

    public void flush() {
        // Note: we must iterate over all assigned partitions here,
        // since time-based rotation might kick in for partitions not present in last call to put().
        for (TopicPartition tp : partitionWriters.keySet()) {
            PartitionWriter partitionWriter = partitionWriters.get(tp);
            try {
                partitionWriter.flush(); // Trigger a flush for this partition
                timeoutMs = MIN_TIMEOUT_MS; // Reset timeout on success
            } catch (RetriableException e) {
                // Reset connector to lastCommittableOffset (if any)
                Long resetOffset = partitionWriter.getLastCommittableOffset();
                if (resetOffset != null) {
                    context.offset(tp, resetOffset);
                }
                // And request a retry in at most timeoutMs
                context.timeout(timeoutMs);
                timeoutMs *= 2; // backoff exponentially
                // And completely reset the partition writer
                partitionWriters.put(tp, newPartitionWriter(tp));
            }
        }
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> preCommit(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        flush(currentOffsets);
        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
        for (TopicPartition tp : partitionWriters.keySet()) {
            Long committableOffset = partitionWriters.get(tp).getLastCommittableOffset();
            if (committableOffset != null) {
                // Only commit offsets that where successfully written to target
                log.trace("Mark {} : {} as ready to commit.", tp, committableOffset);
                offsetsToCommit.put(tp, new OffsetAndMetadata(committableOffset));
            }
        }
        // TODO: return null for DISK storage mode?
        return offsetsToCommit;
    }
}
