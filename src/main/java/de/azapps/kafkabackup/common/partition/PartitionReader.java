package de.azapps.kafkabackup.common.partition;

import de.azapps.kafkabackup.common.record.Record;
import de.azapps.kafkabackup.common.segment.SegmentIndex;
import de.azapps.kafkabackup.common.segment.SegmentReader;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class PartitionReader {
    private final String topic;
    private final int partition;
    private final Path topicDir;
    private SegmentReader currentSegment;
    private final PartitionIndex partitionIndex;

    public PartitionReader(String topic, int partition, Path topicDir) throws IOException, PartitionIndex.IndexException, PartitionException, SegmentIndex.IndexException {
        this.topic = topic;
        this.partition = partition;
        this.topicDir = topicDir;
        Path indexFile = PartitionUtils.indexFile(topicDir, partition);
        if (!Files.isDirectory(this.topicDir)) {
            throw new PartitionException("Cannot find topic directory for topic " + topic);
        }
        if (!Files.isRegularFile(indexFile)) {
            throw new PartitionException("Cannot find index file for partition " + partition);
        }
        partitionIndex = new PartitionIndex(indexFile);
        if (partitionIndex.hasMoreData()) {
            seek(partitionIndex.firstOffset());
        }
    }

    public void close() throws IOException {
        partitionIndex.close();
        if (currentSegment != null) {
            currentSegment.close();
        }
    }

    public void seek(long offset) throws PartitionIndex.IndexException, IOException, SegmentIndex.IndexException, IndexOutOfBoundsException {
        partitionIndex.seek(offset);
        String segmentFilePrefix = partitionIndex.readFileName();
        currentSegment = new SegmentReader(topic, partition, topicDir, segmentFilePrefix);
        currentSegment.seek(offset);
    }

    public boolean hasMoreData() throws IOException {
        if (currentSegment != null) {
            return currentSegment.hasMoreData() || partitionIndex.hasMoreData();
        } else {
            return false;
        }
    }

    public Record read() throws IOException, SegmentIndex.IndexException {
        if (currentSegment.hasMoreData()) {
            return currentSegment.read();
        } else if (partitionIndex.hasMoreData()) {
            currentSegment.close();
            String segmentFilePrefix = partitionIndex.readFileName();
            currentSegment = new SegmentReader(topic, partition, topicDir, segmentFilePrefix);
            return currentSegment.read();
        } else {
            throw new IndexOutOfBoundsException("No more data available");
        }
    }

    public List<Record> readN(int n) throws IOException, SegmentIndex.IndexException {
        List<Record> records = new ArrayList<>();
        while (hasMoreData() && records.size() < n) {
            Record record = read();
            records.add(record);
        }
        return records;
    }

    public List<Record> readBytesBatch(long batchsize) throws IOException, SegmentIndex.IndexException {
        List<Record> records = new ArrayList<>();
        long currentSize = 0;
        while (hasMoreData() && currentSize < batchsize) {
            Record record = read();
            records.add(record);
            if (record.value() != null) {
                currentSize += record.value().length;
            }
            if (record.key() != null) {
                currentSize += record.key().length;
            }
        }
        return records;
    }


    public List<Record> readFully() throws IOException, SegmentIndex.IndexException {
        List<Record> records = new ArrayList<>();
        while (hasMoreData()) {
            Record record = read();
            records.add(record);
        }
        return records;
    }


    public static class PartitionException extends Exception {
        PartitionException(String message) {
            super(message);
        }
    }
}
