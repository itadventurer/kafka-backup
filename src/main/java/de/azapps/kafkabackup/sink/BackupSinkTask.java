package de.azapps.kafkabackup.sink;

import de.azapps.kafkabackup.common.offset.EndOffsetReader;
import de.azapps.kafkabackup.common.offset.OffsetSink;
import de.azapps.kafkabackup.common.partition.PartitionIndex;
import de.azapps.kafkabackup.common.partition.PartitionWriter;
import de.azapps.kafkabackup.common.record.Record;
import de.azapps.kafkabackup.common.segment.SegmentIndex;
import de.azapps.kafkabackup.common.segment.SegmentWriter;
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
import java.util.*;

public class BackupSinkTask extends SinkTask {
    private static final Long DEFAULT_OFFSET = Long.valueOf(-1L);
    private static final Logger log = LoggerFactory.getLogger(BackupSinkTask.class);
    private Path targetDir;
    private Map<TopicPartition, PartitionWriter> partitionWriters = new HashMap<>();
    private long maxSegmentSizeBytes;
    private OffsetSink offsetSink;
    private BackupSinkConfig config;
    private Map<TopicPartition, Long> endOffsets;
    private Map<TopicPartition, Long> currentOffsets = new HashMap<>();
    private EndOffsetReader endOffsetReader;
    private java.util.function.Consumer<Integer> exitFunction;
    private Set<TopicPartition> topicPartitionsWithDefaultOffset = new HashSet();

    @Override
    public String version() {
        return "0.1";
    }

    @Override
    public void start(Map<String, String> props) {
        start(props, null, null, null);
    }

    public void start(
            Map<String, String> props,
            OffsetSink overrideOffsetSink,
            EndOffsetReader overrideEndOffsetReader,
            java.util.function.Consumer<Integer> overrideExitFunction
    ) {
        this.config = new BackupSinkConfig(props);

        try {
            maxSegmentSizeBytes = config.maxSegmentSizeBytes();
            targetDir = Paths.get(config.targetDir());
            Files.createDirectories(targetDir);

            // Allow tests to use mock offset sync
            if(overrideOffsetSink != null) {
                offsetSink = overrideOffsetSink;
            } else {
                AdminClient adminClient = AdminClient.create(config.adminConfig());
                offsetSink = new OffsetSink(adminClient, targetDir);
            }

            if (overrideEndOffsetReader != null) {
                this.endOffsetReader = overrideEndOffsetReader;
            } else {
                endOffsetReader = new EndOffsetReader(config.consumerConfig());
            }

            if (overrideExitFunction != null) {
                this.exitFunction = overrideExitFunction;
            } else {
                this.exitFunction = System::exit;
            }

            log.debug("Initialized BackupSinkTask with target dir {}", targetDir);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Check for end-offsets. Terminate if all offsets >= end-offsets
     */
    private void terminateIfCompleted() {
        boolean terminate = true;
        for (Map.Entry<TopicPartition, Long> partitionOffset : endOffsets.entrySet()) {
            Long endOffset = partitionOffset.getValue();
            Long currentOffset = currentOffsets.getOrDefault(partitionOffset.getKey(), DEFAULT_OFFSET);

            final TopicPartition topicPartition = partitionOffset.getKey();
            log.debug("check to terminate: partition {} current offset {} end offset {}", topicPartition, currentOffset, endOffset);
            boolean topicPartitionIsEmpty = false;

            if (DEFAULT_OFFSET.equals(currentOffset)) {
                if (topicPartitionsWithDefaultOffset.contains(topicPartition)) {
                    topicPartitionIsEmpty = true;
                    log.info("topic partition is empty {}", topicPartition);
                } else {
                    topicPartitionsWithDefaultOffset.add(topicPartition);
                }
            }

            if (!topicPartitionIsEmpty && currentOffset < endOffset - 1) {
                return;
            }
        }
        if (terminate) {
            log.debug("Snapshot complete. Terminating kafka connect.");
            stop(); // seems that this is not called when using System.exit()
            exitFunction.accept(0);
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        try {
            for (SinkRecord sinkRecord : records) {
                TopicPartition topicPartition = new TopicPartition(sinkRecord.topic(), sinkRecord.kafkaPartition());
                PartitionWriter partition = partitionWriters.get(topicPartition);
                partition.append(Record.fromSinkRecord(sinkRecord));
                if (sinkRecord.kafkaOffset() % 100 == 0) {
                    log.debug("Backed up Topic {}, Partition {}, up to offset {}", sinkRecord.topic(), sinkRecord.kafkaPartition(), sinkRecord.kafkaOffset());
                }
                if (config.snapShotMode()) {
                    currentOffsets.put(topicPartition, sinkRecord.kafkaOffset());
                }
            }

            // Todo: refactor to own worker. E.g. using the scheduler of MM2
            offsetSink.syncConsumerGroups();
            offsetSink.syncOffsets();

            if (config.snapShotMode()) {
                terminateIfCompleted();
            }
        } catch (IOException | SegmentIndex.IndexException | PartitionIndex.IndexException | SegmentWriter.SegmentException e) {
            throw new RuntimeException(e);
        }
    }



    public void open(Collection<TopicPartition> partitions) {
        super.open(partitions);
        try {
            for (TopicPartition topicPartition : partitions) {
                Path topicDir = Paths.get(targetDir.toString(), topicPartition.topic());
                Files.createDirectories(topicDir);
                PartitionWriter partitionWriter = new PartitionWriter(topicPartition.topic(), topicPartition.partition(), topicDir, maxSegmentSizeBytes);
                long lastWrittenOffset = partitionWriter.lastWrittenOffset();

                // Note that we must *always* request that we seek to an offset here. Currently the
                // framework will still commit Kafka offsets even though we track our own (see KAFKA-3462),
                // which can result in accidentally using that offset if one was committed but no files
                // were written to disk. To protect against this, even if we
                // just want to start at offset 0 or reset to the earliest offset, we specify that
                // explicitly to forcibly override any committed offsets.

                if (lastWrittenOffset > 0) {
                    context.offset(topicPartition, lastWrittenOffset + 1);
                    log.debug("Initialized Topic {}, Partition {}. Last written offset: {}"
                            , topicPartition.topic(), topicPartition.partition(), lastWrittenOffset);
                } else {
                    // The offset was not found, so rather than forcibly set the offset to 0 we let the
                    // consumer decide where to start based upon standard consumer offsets (if available)
                    // or the consumer's `auto.offset.reset` configuration

                    // if we are in snapshot mode, then just start at zero.
                    if (config.snapShotMode()) {
                        context.offset(topicPartition, 0);
                    }

                    log.info("Resetting offset for {} based upon existing consumer group offsets or, if "
                            + "there are none, the consumer's 'auto.offset.reset' value.", topicPartition);
                }

                this.partitionWriters.put(topicPartition, partitionWriter);
                this.currentOffsets.put(topicPartition, lastWrittenOffset);
            }
            if ( config.snapShotMode() ) {
                this.endOffsets = endOffsetReader.getEndOffsets(partitions);
                this.terminateIfCompleted();
            }
            if (partitions.isEmpty()) {
                log.info("No partitions assigned to BackupSinkTask");
            }
        } catch (IOException | SegmentIndex.IndexException | PartitionIndex.IndexException e) {
            throw new RuntimeException(e);
        }
    }

    public void close(Collection<TopicPartition> partitions) {
        super.close(partitions);
        try {
            for (TopicPartition topicPartition : partitions) {
                PartitionWriter partitionWriter = partitionWriters.get(topicPartition);
                if (partitionWriter != null) {
                    partitionWriter.close();
                }
                partitionWriters.remove(topicPartition);
                log.debug("Closed BackupSinkTask for Topic {}, Partition {}"
                        , topicPartition.topic(), topicPartition.partition());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void stop() {
        try {
            for (PartitionWriter partition : partitionWriters.values()) {
                partition.close();
            }
            offsetSink.close();
            log.info("Stopped BackupSinkTask");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        try {
            for (PartitionWriter partitionWriter : partitionWriters.values()) {
                partitionWriter.flush();
                log.debug("Flushed Topic {}, Partition {}"
                        , partitionWriter.topic(), partitionWriter.partition());
            }
            offsetSink.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
