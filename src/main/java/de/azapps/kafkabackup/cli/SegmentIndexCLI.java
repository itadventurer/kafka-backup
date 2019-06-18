package de.azapps.kafkabackup.cli;

import de.azapps.kafkabackup.common.segment.SegmentIndex;
import de.azapps.kafkabackup.common.segment.SegmentIndexEntry;
import de.azapps.kafkabackup.common.segment.SegmentIndexRestore;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.kafka.common.utils.Exit;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

public class SegmentIndexCLI {
    private static final String CMD_LIST = "list";
    private static final String CMD_LIST_HELP = "List all Records in a segment";
    private static final String ARG_SEGMENT_INDEX = "segment-index";
    private static final String ARG_SEGMENT_INDEX_HELP = "Segment index file (of the form segment_partition_xxx_from_offset_xxxxx_index)";
    private static final String CMD_RESTORE = "restore-index";
    private static final String CMD_RESTORE_HELP = "Restores the segment index given the segment file";
    private static final String ARG_SEGMENT = "segment";
    private static final String ARG_SEGMENT_HELP = "Segment file (of the form segment_partition_xxx_from_offset_xxxxxx_records)";
    private static final String[] COMMANDS = {CMD_LIST, CMD_RESTORE};

    public static void main(String[] args) throws Exception {
        /*
        cli --list --segment-index [file]
        cli --restore-index --segment [file]
        // ideas for later
        cli --show --segment-index [file] --offset [offset]
        cli --validate --segment-index [file] --segment [file]
         */
        final OptionParser optionParser = new OptionParser();
        // Commands
        optionParser.accepts(CMD_LIST, CMD_LIST_HELP);
        optionParser.accepts(CMD_RESTORE, CMD_RESTORE_HELP);
        // Arguments
        optionParser.accepts(ARG_SEGMENT_INDEX, ARG_SEGMENT_INDEX_HELP)
                .requiredIf(CMD_LIST)
                .withRequiredArg().ofType(String.class);
        optionParser.accepts(ARG_SEGMENT, ARG_SEGMENT_HELP)
                .requiredIf(CMD_RESTORE)
                .withRequiredArg().ofType(String.class);

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
            list((String) options.valueOf(ARG_SEGMENT_INDEX));
        } else if (options.has(CMD_RESTORE)) {
            restore((String) options.valueOf(ARG_SEGMENT));
        }
    }

    private static void restore(String segmentFileName) throws SegmentIndex.IndexException, SegmentIndexRestore.RestoreException, IOException {
        if (!segmentFileName.endsWith("_records")) {
            segmentFileName += "_records";
        }
        SegmentIndexRestore restore = new SegmentIndexRestore(Paths.get(segmentFileName));
        restore.restore();
    }

    private static void list(String segmentIndexFileName) throws IOException, SegmentIndex.IndexException {
        if (!segmentIndexFileName.endsWith("_index")) {
            segmentIndexFileName += "_index";
        }
        SegmentIndex segmentIndex = new SegmentIndex(Paths.get(segmentIndexFileName));
        List<SegmentIndexEntry> index = segmentIndex.index();
        long previousOffset = index.get(0).getOffset() - 1;
        for (SegmentIndexEntry entry : index) {
            System.out.print(String.format("Offset: %d Position: %d Length: %d", entry.getOffset(), entry.recordFilePosition(), entry.recordByteLength()));
            if (entry.getOffset() > previousOffset + 1) {
                System.out.print(" <- FYI Here is a gap");
            }
            System.out.println();
            previousOffset = entry.getOffset();
        }
        System.out.println(String.format("%d entries in Index", index.size()));
    }
}
