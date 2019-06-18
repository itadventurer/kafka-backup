package de.azapps.kafkabackup.common.partition;

import de.azapps.kafkabackup.common.segment.SegmentUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class PartitionIndexRestore {
    private final Path indexFile;
    private final int partition;
    PartitionIndex index;
    Path topicDir;

    public PartitionIndexRestore(Path topicDir, int partition) throws RestoreException, IOException, PartitionIndex.IndexException {
        this.topicDir = topicDir;
        this.indexFile = PartitionUtils.indexFile(topicDir, partition);
        this.partition = partition;

        if (Files.isRegularFile(indexFile)) {
            throw new RestoreException("Index file " + indexFile + " must not exist");
        }
        index = new PartitionIndex(indexFile);
        if (!Files.isDirectory(topicDir)) {
            throw new RuntimeException("Topic directory " + topicDir + " does not exist");
        }
    }

    public void restore() throws IOException {
        Files.list(topicDir)
                .filter(x -> SegmentUtils.isSegment(x)
                        && SegmentUtils.getPartitionFromSegment(x) == partition)
                .sorted()
                .forEach((Path f) -> {

                    long offset = SegmentUtils.getStartOffsetFromSegment(f);
                    try {
                        index.appendSegment(SegmentUtils.filePrefix(partition, offset), offset);
                    } catch (IOException | PartitionIndex.IndexException e) {
                        throw new RuntimeException(e);
                    }
                });
        index.flush();
        index.close();
    }


    public static class RestoreException extends Exception {
        RestoreException(String message) {
            super(message);
        }
    }
}
