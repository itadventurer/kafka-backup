package de.azapps.kafkabackup.common.partition;

import de.azapps.kafkabackup.common.record.Record;
import de.azapps.kafkabackup.common.segment.SegmentWriter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;

public class PartitionReader {
    private String topic;
    private int partition;
    private Path topicDir;
    private Optional<SegmentWriter> currentSegment = Optional.empty();
    private PartitionIndex partitionIndex;

    public PartitionReader(String topic, int partition, Path topicDir) throws IOException, PartitionIndex.IndexException, PartitionException {
        this.topic = topic;
        this.partition = partition;
        this.topicDir = topicDir;
        Path indexFile = Paths.get(topicDir.toString(), "index_partition_" + partition);
        if (!Files.isDirectory(this.topicDir)) {
            throw new PartitionException("Cannot find topic directory for topic " + topic);
        }
        if (!Files.isRegularFile(indexFile)) {
            throw new PartitionException("Cannot find index file for partition " + partition);
        }
        partitionIndex = new PartitionIndex(indexFile);
    }

    public void close() throws IOException {
        partitionIndex.close();
        if (currentSegment.isPresent()) {
            currentSegment.get().close();
        }
    }

    /*public List<Record> getRecords(long startOffset, int batchSize) throws PartitionIndex.IndexException {
        String segmentFilePrefix = partitionIndex.fileForOffset(startOffset);
    }
*/

    public static class PartitionException extends Exception {
        PartitionException(String message) {
            super(message);
        }
    }
}
