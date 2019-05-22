package de.azapps.kafkabackup.source;

import de.azapps.kafkabackup.common.Constants;
import de.azapps.kafkabackup.common.partition.PartitionIndex;
import de.azapps.kafkabackup.common.partition.PartitionReader;
import de.azapps.kafkabackup.common.record.Record;
import de.azapps.kafkabackup.common.segment.SegmentIndex;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BackupSourceTask extends SourceTask {
    private Path sourceDir;
    private Map<TopicPartition, PartitionReader> partitionReaders = new HashMap<>();
    private int batchSize = 100;

    @Override
    public String version() {
        return Constants.VERSION;
    }

    @Override
    public void start(Map<String, String> props) {
        if (!props.containsKey(Constants.SOURCE_DIR_CONFIG)) {
            throw new RuntimeException("Missing Configuration Variable: " + Constants.SOURCE_DIR_CONFIG);
        }
        sourceDir = Paths.get(props.get(Constants.SOURCE_DIR_CONFIG));
        if (props.containsKey(Constants.BATCH_SIZE_CONFIG)) {
            batchSize = Integer.parseInt(props.get(Constants.BATCH_SIZE_CONFIG));
        }
        if (!props.containsKey(Constants.TOPICS)) {
            throw new RuntimeException("Missing Configuration Variable: " + Constants.TOPICS);
        }
        String[] topics = props.get(Constants.TOPICS).split("\\s*,\\s*");
        for (String topic : topics) {
            Path topicDir = Paths.get(sourceDir.toString(), topic);
            if (!Files.isDirectory(topicDir)) {
                throw new RuntimeException("Missing directory for topic " + topic);
            }
            try {
                Files.list(topicDir).forEach((Path f) -> {
                    String fname = f.getFileName().toString();
                    Pattern MY_PATTERN = Pattern.compile("index_partition_([0-9]+)");
                    Matcher m = MY_PATTERN.matcher(fname);
                    if (m.find()) {
                        String partitionStr = m.group(1);
                        int partition = Integer.valueOf(partitionStr);
                        TopicPartition topicPartition = new TopicPartition(topic, partition);
                        PartitionReader partitionReader;
                        try {
                            partitionReader = new PartitionReader(topic, partition, topicDir);
                        } catch (IOException | PartitionIndex.IndexException | PartitionReader.PartitionException | SegmentIndex.IndexException e) {
                            throw new RuntimeException(e);
                        }
                        partitionReaders.put(topicPartition, partitionReader);
                        System.out.println("Registered topic " + topic + " partition " + partition);
                    }
                });
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        List<SourceRecord> sourceRecords = new ArrayList<>();
        try {
            for (PartitionReader partitionReader : partitionReaders.values()) {
                List<Record> records = partitionReader.readN(batchSize);
                if(records.size() > 0) {
                    System.out.println("Read " + records.size() + " record from topic " + records.get(0).topic() + " partition " + records.get(0).kafkaPartition());
                }
                for (Record record : records) {
                    sourceRecords.add(toSourceRecord(record));
                }
            }
        } catch (IOException | SegmentIndex.IndexException e) {
            e.printStackTrace();
        }
        return sourceRecords;
    }

    private SourceRecord toSourceRecord(Record record) {
        Map<String, String> sourcePartition = new HashMap<>();
        //sourcePartition.put("Partition", record.kafkaPartition().toString());
        //sourcePartition.put("Topic", record.topic());
        Map<String, Long> sourceOffset = new HashMap<>();//Collections.singletonMap("Offset", record.kafkaOffset());
        return new SourceRecord(sourcePartition, sourceOffset,
                record.topic(), record.kafkaPartition(),
                Schema.BYTES_SCHEMA, record.key(),
                Schema.BYTES_SCHEMA, record.value(),
                record.timestamp(), record.headers());
    }
    public void commit() throws InterruptedException {
        // This space intentionally left blank.
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
    }
}
