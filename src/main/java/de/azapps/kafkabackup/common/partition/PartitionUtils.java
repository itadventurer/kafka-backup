package de.azapps.kafkabackup.common.partition;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PartitionUtils {
    private static final Pattern PARTITION_INDEX_PATTERN = Pattern.compile("^index_partition_([0-9]+)$");

    static Path indexFile(Path topicDir, int partition) {
        return Paths.get(topicDir.toString(), String.format("index_partition_%03d", partition));
    }

    public static Optional<Integer> isPartitionIndex(Path f) {
        Path fpath = f.getFileName();
        if (fpath == null) {
            return Optional.empty();
        }
        String fname = fpath.toString();
        Matcher m = PARTITION_INDEX_PATTERN.matcher(fname);
        if (m.find()) {
            String partitionStr = m.group(1);
            return Optional.of(Integer.valueOf(partitionStr));
        } else {
            return Optional.empty();
        }
    }
}
