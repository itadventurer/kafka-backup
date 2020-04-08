package de.azapps.kafkabackup.common.partition.disk;

import de.azapps.kafkabackup.common.partition.PartitionWriter;
import de.azapps.kafkabackup.common.partition.PartitionException;
import de.azapps.kafkabackup.common.record.Record;
import de.azapps.kafkabackup.common.segment.SegmentIndex;
import de.azapps.kafkabackup.common.segment.SegmentWriter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

public class DiskPartitionWriter implements PartitionWriter {
    private String topic;
    private int partition;
    private Path topicDir;
    private SegmentWriter currentSegment;
    private PartitionIndex partitionIndex;
    private long maxSegmentSize;

    public DiskPartitionWriter(String topic, int partition, Path topicDir, long maxSegmentSize) throws IOException, PartitionIndex.IndexException, SegmentIndex.IndexException {
        this.topic = topic;
        this.partition = partition;
        this.topicDir = topicDir;
        this.maxSegmentSize = maxSegmentSize;
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

    @Override
    public void append(Record record) throws PartitionException {
        try {
            if (currentSegment.size() > maxSegmentSize) {
                nextSegment(record.kafkaOffset());
            }
            currentSegment.append(record);
        } catch (IOException | SegmentIndex.IndexException | PartitionIndex.IndexException | SegmentWriter.SegmentException e) {
            throw new PartitionException(e);
        }
    }

    @Override
    public void close() throws PartitionException {
        try {
            partitionIndex.close();
            currentSegment.close();
        } catch (IOException e) {
            throw new PartitionException(e);
        }
    }

    @Override
    public void flush() throws PartitionException {
        try {
            partitionIndex.flush();
            currentSegment.flush();
        } catch (IOException e) {
            throw new PartitionException(e);
        }
    }

    @Override
    public String topic() {
        return topic;
    }

    @Override
    public int partition() {
        return partition;
    }

    @Override
    public Long getLastCommittableOffset() {
        return lastWrittenOffset() + 1; // committed offset in kafka are the *next to read*.
    }
}
