package de.azapps.kafkabackup.common.offset;

import org.apache.kafka.common.TopicPartition;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class OffsetUtils {

    private static final String OFFSET_STORE_FILE_PREFIX = "consumer_offsets_partition";
    private static final Pattern FILE_PATTERN = Pattern.compile("consumer_offsets_partition_([0-9]+)");

    static String offsetStoreFileName(int partition) {
        return String.format(OFFSET_STORE_FILE_PREFIX + "_%03d", partition);
    }

    static Path offsetStoreFile(Path backupDir, TopicPartition topicPartition) {
        return Paths.get(backupDir.toString(), topicPartition.topic(), OffsetUtils.offsetStoreFileName(topicPartition.partition()));
    }

    static Optional<Integer> isOffsetStoreFile(Path f) {
        Path fpath = f.getFileName();
        if (fpath == null) {
            return Optional.empty();
        }
        String fname = fpath.toString();
        Matcher m = FILE_PATTERN.matcher(fname);
        if (m.find()) {
            String partitionStr = m.group(1);
            return Optional.of(Integer.valueOf(partitionStr));
        } else {
            return Optional.empty();
        }
    }
}
