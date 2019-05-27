package de.azapps.kafkabackup.common.segment;

import java.io.File;
import java.nio.file.Path;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SegmentUtils {

    private static final Pattern SEGMENT_PATTERN = Pattern.compile("^segment_partition_([0-9]+)_from_offset_([0-9]+)_records$");

    public static String filePrefix(int partition, long startOffset) {
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

    public static boolean isSegment(Path file) {
        Matcher m = SEGMENT_PATTERN.matcher(file.getFileName().toString());
        return m.find();
    }

    public static int getPartitionFromSegment(Path file) {
        Matcher m = SEGMENT_PATTERN.matcher(file.getFileName().toString());
        if (m.find()) {
            String partitionStr = m.group(1);
            return Integer.valueOf(partitionStr);
        } else {
            throw new RuntimeException("File " + file + " is not a Segment");
        }
    }

    public static long getStartOffsetFromSegment(Path file) {
        Matcher m = SEGMENT_PATTERN.matcher(file.getFileName().toString());
        if (m.find()) {
            String offsetStr = m.group(2);
            return Long.valueOf(offsetStr);
        } else {
            throw new RuntimeException("File " + file + " is not a Segment");
        }
    }

}
