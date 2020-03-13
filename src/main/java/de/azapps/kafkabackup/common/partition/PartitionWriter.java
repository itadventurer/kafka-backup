package de.azapps.kafkabackup.common.partition;

import de.azapps.kafkabackup.common.record.Record;
import de.azapps.kafkabackup.common.segment.SegmentIndex;
import de.azapps.kafkabackup.common.segment.SegmentWriter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

public class PartitionWriter {
    private String topic;
    private int partition;
    private Path topicDir;
    private SegmentWriter currentSegment;
    private PartitionIndex partitionIndex;
    private long maxSegmentSizeBytes;

    public PartitionWriter(String topic, int partition, Path topicDir, long maxSegmentSizeBytes) throws IOException, PartitionIndex.IndexException, SegmentIndex.IndexException {
        this.topic = topic;
        this.partition = partition;
        this.topicDir = topicDir;
        this.maxSegmentSizeBytes = maxSegmentSizeBytes;
        Path indexFile = PartitionUtils.indexFile(topicDir, partition);
        if (!Files.isDirectory(this.topicDir)) {
            Files.createDirectories(this.topicDir);
        }
        partitionIndex = new PartitionIndex(indexFile);
        Optional<PartitionIndexEntry> optionalPartitionIndexEntry = partitionIndex.latestSegmentFile();
        if (optionalPartitionIndexEntry.isPresent()) {
            currentSegment = new SegmentWriter(topic, partition, optionalPartitionIndexEntry.get().startOffset(), topicDir);
        } else {
            currentSegment = new SegmentWriter(topic, partition, 0, topicDir);
            // do not forget to add the current segment to the partition index. Even if it is empty
            partitionIndex.appendSegment(currentSegment.filePrefix(), 0);
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

    public long lastWrittenOffset() {
        return currentSegment.lastWrittenOffset();
    }

    public void append(Record record) throws IOException, SegmentIndex.IndexException, PartitionIndex.IndexException, SegmentWriter.SegmentException {
        if (currentSegment.size() > maxSegmentSizeBytes) {
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

    public String topic() {
        return topic;
    }

    public int partition() {
        return partition;
    }
}
