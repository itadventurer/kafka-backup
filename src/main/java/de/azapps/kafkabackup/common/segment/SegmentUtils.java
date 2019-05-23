package de.azapps.kafkabackup.common.segment;

import java.io.File;
import java.nio.file.Path;

class SegmentUtils {
    static String filePrefix(int partition, long startOffset) {
        return String.format("segment_partition_%03d_from_offset_%010d", partition, startOffset);
    }

    static File indexFile(Path topicDir, int partition, long startOffset) {
        return indexFile(topicDir, filePrefix(partition, startOffset));
    }

    static File indexFile(Path topicDir, String filePrefix) {
        return new File(topicDir.toFile(), filePrefix + "_index");
    }

    static File recordsFile(Path topicDir, int partition, long startOffset) {
        return recordsFile(topicDir, filePrefix(partition, startOffset));
    }

    static File recordsFile(Path topicDir, String filePrefix) {
        return new File(topicDir.toFile(), filePrefix + "_records");
    }

}
