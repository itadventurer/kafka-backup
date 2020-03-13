package de.azapps.kafkabackup.cli;

import de.azapps.kafkabackup.common.partition.PartitionIndex;
import de.azapps.kafkabackup.common.partition.PartitionIndexEntry;
import de.azapps.kafkabackup.common.partition.PartitionIndexRestore;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.kafka.common.utils.Exit;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.stream.Stream;

public class PartitionIndexCLI {
    private static final String CMD_LIST = "list";
    private static final String CMD_RESTORE = "restore-index";
    private static final String[] COMMANDS = {CMD_LIST, CMD_RESTORE};
    private static final String ARG_PARTITION_INDEX = "partition-index";
    private static final String ARG_TOPIC_DIR = "topic-dir";
    private static final String ARG_PARTITION = "partition";

    public static void main(String[] args) throws Exception {
        /*
        cli --list --partition-index [file]
        cli --restore-index --partition 0 --topic-dir [dir]
        // ideas for later
        cli --validate --partition-index [file] --partition 0 --topic-dir [dir]
         */
        final OptionParser optionParser = new OptionParser();
        // Commands
        optionParser.accepts(CMD_LIST);
        optionParser.accepts(CMD_RESTORE);
        // Arguments
        optionParser.accepts(ARG_PARTITION_INDEX)
                .requiredIf(CMD_LIST)
                .withRequiredArg().ofType(String.class);
        optionParser.accepts(ARG_TOPIC_DIR)
                .requiredIf(CMD_RESTORE)
                .withRequiredArg().ofType(String.class);
        optionParser.accepts(ARG_PARTITION)
                .requiredIf(CMD_RESTORE)
                .withRequiredArg().ofType(Integer.class);

        OptionSet options;
        try {
            options = optionParser.parse(args);
            if (Stream.of(COMMANDS).filter(options::has).count() != 1) {
                throw new Exception("Must contain exactly one of " + Arrays.toString(COMMANDS));
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            optionParser.printHelpOn(System.err);
            Exit.exit(-1);
            return;
        }
        if (options.has(CMD_LIST)) {
            list((String) options.valueOf(ARG_PARTITION_INDEX));
        } else if (options.has(CMD_RESTORE)) {
            restore((String) options.valueOf(ARG_TOPIC_DIR), (Integer) options.valueOf(ARG_PARTITION));
        } else {
            optionParser.printHelpOn(System.err);
        }
    }

    private static void restore(String topicDir, int partition) throws PartitionIndex.IndexException, PartitionIndexRestore.RestoreException, IOException {
        PartitionIndexRestore restore = new PartitionIndexRestore(Paths.get(topicDir), partition);
        restore.restore();
    }

    private static void list(String partitionIndexFileName) throws IOException, PartitionIndex.IndexException {
        System.out.println(partitionIndexFileName);
        PartitionIndex partitionIndex = new PartitionIndex(Paths.get(partitionIndexFileName));
        for (PartitionIndexEntry entry : partitionIndex.index()) {
            System.out.println(String.format("File: %s StartOffset: %d", entry.filename(), entry.startOffset()));
        }
    }
}
