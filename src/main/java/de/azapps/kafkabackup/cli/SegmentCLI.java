package de.azapps.kafkabackup.cli;

import de.azapps.kafkabackup.common.record.Record;
import de.azapps.kafkabackup.common.segment.UnverifiedSegmentReader;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.kafka.common.utils.Exit;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Stream;

public class SegmentCLI {
    private static final String CMD_LIST = "list";
    private static final String ARG_SEGMENT = "segment";
    private static final String ARG_SEGMENT_HELP = "Segment file (of the form segment_partition_xxx_from_offset_xxxxxx_records)";
    private static final String CMD_SHOW = "show";
    private static final String ARG_OFFSET = "offset";
    private static final String CMD_LIST_HELP = "Lists all records in the segment. Just counting the value length – not displaying it.";
    private static final String CMD_SHOW_HELP = "Shows a specific record in the segment. Displays key and value";
    private static final String[] COMMANDS = {CMD_LIST, CMD_SHOW};

    public static void main(String[] args) throws IOException {
        /*
        cli --list --segment [file]
        cli --show --segment [file] --partition 0
         */
        final OptionParser optionParser = new OptionParser();
        optionParser.accepts(ARG_SEGMENT, ARG_SEGMENT_HELP).withRequiredArg().ofType(String.class);
        optionParser.accepts(CMD_LIST, CMD_LIST_HELP);
        optionParser.accepts(CMD_SHOW, CMD_SHOW_HELP);
        optionParser.accepts(ARG_OFFSET).requiredIf(CMD_SHOW).withRequiredArg().ofType(Long.class);


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

        String segmentIndexFileName = (String) options.valueOf(ARG_SEGMENT);
        if (!segmentIndexFileName.endsWith("_records")) {
            segmentIndexFileName += "_records";
        }
        UnverifiedSegmentReader segmentReader = new UnverifiedSegmentReader(new File(segmentIndexFileName));

        if (options.has(CMD_LIST)) {
            list(segmentReader);
        } else if (options.has(CMD_SHOW)) {
            show(segmentReader, (Long) options.valueOf(ARG_OFFSET));
        }

    }

    private static void show(UnverifiedSegmentReader segmentReader, Long offset) {
        long maxOffset = -1;
        while (true) {
            try {
                Record record = segmentReader.read();
                maxOffset = record.kafkaOffset();
                if (record.kafkaOffset() == offset) {
                    System.out.println(record.key() + ", " + record.value());
                    Exit.exit(0);
                }
            } catch (EOFException e) {
                System.out.println("Did not found offset " + offset + " in file. Max offset is " + maxOffset);
                Exit.exit(-2);
            } catch (IOException e) {
                e.printStackTrace();
                Exit.exit(-3);
            }
        }

    }

    private static void list(UnverifiedSegmentReader segmentReader) {
        int cnt = 0;
        while (true) {
            try {
                Record record = segmentReader.read();
                System.out.println("Offset: " + record.kafkaOffset() +
                        " Key: " + record.key() +
                        " Data Length: " + record.value().length);
                cnt++;
            } catch (EOFException e) {
                break;
            } catch (IOException e) {
                e.printStackTrace();
                Exit.exit(-2);
            }
        }
        System.out.println(String.format("%d entries in Segment", cnt));

    }
}
