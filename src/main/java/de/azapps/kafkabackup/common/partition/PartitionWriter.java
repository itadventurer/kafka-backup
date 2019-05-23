package de.azapps.kafkabackup.common.partition;

import de.azapps.kafkabackup.common.record.Record;
import de.azapps.kafkabackup.common.segment.SegmentIndex;
import de.azapps.kafkabackup.common.segment.SegmentWriter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

public class PartitionWriter {
    private String topic;
    private int partition;
    private Path topicDir;
    private SegmentWriter currentSegment;
    private PartitionIndex partitionIndex;
    private long maxSegmentSize;

    public PartitionWriter(String topic, int partition, Path topicDir, long maxSegmentSize) throws IOException, PartitionIndex.IndexException, SegmentIndex.IndexException {
        this.topic = topic;
        this.partition = partition;
        this.topicDir = topicDir;
        this.maxSegmentSize = maxSegmentSize;
        Path indexFile = PartitionUtils.indexFile(topicDir, partition);
        if (!Files.isDirectory(this.topicDir)) {
            Files.createDirectories(this.topicDir);
        }
        if (!Files.isRegularFile(indexFile)) {
            Files.createFile(indexFile);
        }
        partitionIndex = new PartitionIndex(indexFile);
        Optional<PartitionIndexEntry> optionalPartitionIndexEntry = partitionIndex.latestSegmentFile();
        if (optionalPartitionIndexEntry.isPresent()) {
            currentSegment = new SegmentWriter(topic, partition, optionalPartitionIndexEntry.get().startOffset(), topicDir);
        } else {
            currentSegment = new SegmentWriter(topic, partition, 0, topicDir);
        }
    }

    private void nextSegment(long startOffset) throws IOException, SegmentIndex.IndexException, PartitionIndex.IndexException {
        currentSegment.close();
        SegmentWriter segment = new SegmentWriter(topic, partition, startOffset, topicDir);
        if (startOffset > partitionIndex.latestStartOffset()) {
            partitionIndex.appendSegment(segment.filePrefix(), startOffset);
        }
        currentSegment = segment;
    }

    public void append(Record record) throws IOException, SegmentIndex.IndexException, PartitionIndex.IndexException, SegmentWriter.SegmentException {
        if (currentSegment.size() > maxSegmentSize) {
            nextSegment(record.kafkaOffset());
        }
        currentSegment.append(record);
    }

    public void close() throws IOException {
        partitionIndex.close();
        currentSegment.close();
    }

    public void flush() throws IOException {
        partitionIndex.flush();
        currentSegment.flush();
    }
}
