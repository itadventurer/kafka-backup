package de.azapps.kafkabackup.common.segment;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SegmentUtils {

    static final byte V1_MAGIC_BYTE = 0x01;
    private static final Pattern SEGMENT_PATTERN = Pattern.compile("^segment_partition_([0-9]+)_from_offset_([0-9]+)_records$");

    public static String filePrefix(int partition, long startOffset) {
        return String.format("segment_partition_%03d_from_offset_%010d", partition, startOffset);
    }

    static void ensureValidSegment(FileInputStream inputStream) throws IOException {
        inputStream.getChannel().position(0);
        byte[] v1Validation = new byte[1];
        if (inputStream.read(v1Validation) != 1 || v1Validation[0] != SegmentUtils.V1_MAGIC_BYTE) {
            throw new IOException("Cannot validate Magic Byte in the beginning of the Segment");
        }
    }

    public static Path indexFile(Path topicDir, int partition, long startOffset) {
        return indexFile(topicDir, filePrefix(partition, startOffset));
    }

    static Path indexFile(Path topicDir, String filePrefix) {
        return Paths.get(topicDir.toString(), filePrefix + "_index");
    }

    public static Path recordsFile(Path topicDir, int partition, long startOffset) {
        return recordsFile(topicDir, filePrefix(partition, startOffset));
    }

    static Path recordsFile(Path topicDir, String filePrefix) {
        return Paths.get(topicDir.toString(), filePrefix + "_records");
    }

    public static boolean isSegment(Path file) {
        Path fpath = file.getFileName();
        if (fpath == null) {
            return false;
        }
        Matcher m = SEGMENT_PATTERN.matcher(fpath.toString());
        return m.find();
    }

    public static int getPartitionFromSegment(Path file) {
        Path fpath = file.getFileName();
        if (fpath == null) {
            throw new RuntimeException("File " + file + " is not a Segment");
        }
        Matcher m = SEGMENT_PATTERN.matcher(fpath.toString());
        if (m.find()) {
            String partitionStr = m.group(1);
            return Integer.parseInt(partitionStr);
        } else {
            throw new RuntimeException("File " + file + " is not a Segment");
        }
    }

    public static long getStartOffsetFromSegment(Path file) {
        Path fpath = file.getFileName();
        if (fpath == null) {
            throw new RuntimeException("File " + file + " is not a Segment");
        }
        Matcher m = SEGMENT_PATTERN.matcher(fpath.toString());
        if (m.find()) {
            String offsetStr = m.group(2);
            return Long.parseLong(offsetStr);
        } else {
            throw new RuntimeException("File " + file + " is not a Segment");
        }
    }

}
