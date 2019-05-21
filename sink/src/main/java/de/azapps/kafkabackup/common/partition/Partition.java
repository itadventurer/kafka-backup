package de.azapps.kafkabackup.common.partition;

import de.azapps.kafkabackup.common.record.Record;
import de.azapps.kafkabackup.common.segment.Segment;
import de.azapps.kafkabackup.common.segment.SegmentIndex;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

public class Partition {
    private String topic;
    private int partition;
    private Path topicDir;
    private Optional<Segment> currentSegment = Optional.empty();
    private PartitionIndex partitionIndex;
    private long maxSegmentSize;

    public Partition(String topic, int partition, Path topicDir, boolean forWriting, long maxSegmentSize) throws IOException, PartitionIndex.IndexException, PartitionException {
        this.topic = topic;
        this.partition = partition;
        this.topicDir = topicDir;
        Path indexFile = Paths.get(topicDir.toString(), "index_partition_" + partition);
        if (!Files.isDirectory(this.topicDir)) {
            if (!forWriting) {
                throw new PartitionException("Cannot find topic directory for topic " + topic);
            }
            Files.createDirectories(this.topicDir);
        }
        if (!Files.isRegularFile(indexFile)) {
            if (!forWriting) {
                throw new PartitionException("Cannot find index file for partition " + partition);
            }
            Files.createFile(indexFile);
        }
        partitionIndex = new PartitionIndex(indexFile);
        this.maxSegmentSize = maxSegmentSize;
    }

    private void nextSegment(long startOffset) throws IOException, SegmentIndex.IndexException {
        if (currentSegment.isPresent()) {
            currentSegment.get().close();
        }
        Segment segment = new Segment(topic, partition, startOffset, topicDir);
        partitionIndex.nextSegment(segment.fileName(), startOffset);
        currentSegment = Optional.of(segment);
    }

    public void append(Record record) throws IOException, SegmentIndex.IndexException, PartitionIndex.IndexException {
        if (currentSegment.isEmpty() || currentSegment.get().size() > maxSegmentSize) {
            nextSegment(record.kafkaOffset());
        }
        currentSegment.get().append(record);
    }

    public void close() throws IOException {
        partitionIndex.close();
        if (currentSegment.isPresent()) {
            currentSegment.get().close();
        }

    }

    public void flush() throws IOException {
        partitionIndex.flush();
        if (currentSegment.isPresent()) {
            currentSegment.get().flush();
        }
    }


    public static class PartitionException extends Exception {
        PartitionException(String message) {
            super(message);
        }
    }
}
