package de.azapps.kafkabackup.source;

import de.azapps.kafkabackup.common.offset.OffsetSource;
import de.azapps.kafkabackup.common.partition.PartitionIndex;
import de.azapps.kafkabackup.common.partition.PartitionReader;
import de.azapps.kafkabackup.common.partition.PartitionUtils;
import de.azapps.kafkabackup.common.record.Record;
import de.azapps.kafkabackup.common.segment.SegmentIndex;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class BackupSourceTask extends SourceTask {
    private static final Logger log = LoggerFactory.getLogger(BackupSourceTask.class);
    private static final String SOURCE_PARTITION_PARTITION = "Partition";
    private static final String SOURCE_PARTITION_TOPIC = "Topic";
    private static final String SOURCE_OFFSET_OFFSET = "Offset";
    private Path sourceDir;
    private final Map<TopicPartition, PartitionReader> partitionReaders = new HashMap<>();
    private final Set<TopicPartition> finishedPartitions = new HashSet<>();
    private int batchSize = 100;
    private OffsetSource offsetSource;
    private List<String> topics;

    @Override
    public String version() {
        return "0.1";
    }

    @Override
    public void start(Map<String, String> props) {
        BackupSourceConfig config = new BackupSourceConfig(props);
        sourceDir = Paths.get(config.sourceDir());
        batchSize = config.batchSize();
        topics = config.topics();
        try {
            findPartitions();
            offsetSource = new OffsetSource(sourceDir, topics, config.consumerConfig());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        for (Map.Entry<TopicPartition, PartitionReader> entry : partitionReaders.entrySet()) {
            TopicPartition topicPartition = entry.getKey();
            PartitionReader partitionReader = entry.getValue();

            Map<String, String> sourcePartition = new HashMap<>();
            sourcePartition.put(SOURCE_PARTITION_TOPIC, topicPartition.topic());
            sourcePartition.put(SOURCE_PARTITION_PARTITION, String.valueOf(topicPartition.partition()));
            Map<String, Object> sourceOffset = context.offsetStorageReader().offset(sourcePartition);
            if (sourceOffset != null) {
                try {
                    // seek() seeks to the position of the OFFSET. We need to move to the position after the current OFFSET.
                    // Otherwise we would write the OFFSET multiple times in case of a restart
                    partitionReader.seek((Long) sourceOffset.get(SOURCE_OFFSET_OFFSET));
                    if (partitionReader.hasMoreData()) {
                        partitionReader.read();
                    }
                } catch (IOException | SegmentIndex.IndexException | PartitionIndex.IndexException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private void findPartitions() throws IOException {
        for (String topic : topics) {
            Path topicDir = Paths.get(sourceDir.toString(), topic);
            if (!Files.isDirectory(topicDir)) {
                throw new RuntimeException("Missing directory for topic " + topic);
            }
            Files.list(topicDir).forEach((Path f) -> {
                Optional<Integer> partition = PartitionUtils.isPartitionIndex(f);
                if (partition.isPresent()) {
                    TopicPartition topicPartition = new TopicPartition(topic, partition.get());
                    PartitionReader partitionReader;
                    try {
                        partitionReader = new PartitionReader(topic, partition.get(), topicDir);
                    } catch (IOException | PartitionIndex.IndexException | PartitionReader.PartitionException | SegmentIndex.IndexException e) {
                        throw new RuntimeException(e);
                    }
                    partitionReaders.put(topicPartition, partitionReader);
                    log.info("Registered topic {} partition {}", topic, partition);
                }
            });
        }
    }

    long lastPrint = 0;

    @Override
    public List<SourceRecord> poll() {
        List<SourceRecord> sourceRecords = new ArrayList<>();
        if (finishedPartitions.equals(partitionReaders.keySet())) {
            long currentTime = System.currentTimeMillis();
            if (currentTime - lastPrint > 5000) {
                log.info("All records read. Restore was successful");
                lastPrint = currentTime;
            }
            return new ArrayList<>();
        }
        try {
            for (Map.Entry<TopicPartition, PartitionReader> entry : partitionReaders.entrySet()) {
                TopicPartition topicPartition = entry.getKey();
                PartitionReader partitionReader = entry.getValue();

                List<Record> records = partitionReader.readBytesBatch(batchSize);
                if (records.size() > 0) {
                    log.info("Read {} record(s) from topic {} partition {}. Current offset: {}",
                            records.size(), records.get(0).topic(), records.get(0).kafkaPartition(), records.get(records.size() - 1).kafkaOffset());
                }
                for (Record record : records) {
                    sourceRecords.add(toSourceRecord(record));
                }
                if (!partitionReader.hasMoreData()) {
                    finishedPartitions.add(topicPartition);
                }
            }
        } catch (IOException | SegmentIndex.IndexException e) {
            e.printStackTrace();
        }
        return sourceRecords;
    }

    private SourceRecord toSourceRecord(Record record) {
        Map<String, String> sourcePartition = new HashMap<>();
        sourcePartition.put(SOURCE_PARTITION_PARTITION, record.kafkaPartition().toString());
        sourcePartition.put(SOURCE_PARTITION_TOPIC, record.topic());
        Map<String, Long> sourceOffset = Collections.singletonMap(SOURCE_OFFSET_OFFSET, record.kafkaOffset());
        ConnectHeaders connectHeaders = new ConnectHeaders();
        for (Header header : record.headers()) {
            connectHeaders.addBytes(header.key(), header.value());
        }
        return new SourceRecord(sourcePartition, sourceOffset,
                record.topic(), record.kafkaPartition(),
                Schema.OPTIONAL_BYTES_SCHEMA, record.key(),
                Schema.OPTIONAL_BYTES_SCHEMA, record.value(),
                record.timestamp(), connectHeaders);
    }

    @Override
    public void commitRecord(SourceRecord record, RecordMetadata metadata) {
        TopicPartition topicPartition = new TopicPartition(metadata.topic(), metadata.partition());
        long sourceOffset = (Long) record.sourceOffset().get(SOURCE_OFFSET_OFFSET);
        long targetOffset = metadata.offset();
        offsetSource.syncGroupForOffset(topicPartition, sourceOffset, targetOffset);
    }

    @Override
    public void stop() {
        for (PartitionReader partitionReader : partitionReaders.values()) {
            try {
                partitionReader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        log.info("Stopped BackupSourceTask");
    }
}
