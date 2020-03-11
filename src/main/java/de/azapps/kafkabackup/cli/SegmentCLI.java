package de.azapps.kafkabackup.cli;

import de.azapps.kafkabackup.cli.formatters.*;
import de.azapps.kafkabackup.common.record.Record;
import de.azapps.kafkabackup.common.segment.UnverifiedSegmentReader;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.kafka.common.utils.Exit;

import java.io.EOFException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.stream.Stream;

public class SegmentCLI {
    private static final String CMD_LIST = "list";
    private static final String ARG_SEGMENT = "segment";
    private static final String ARG_SEGMENT_HELP = "Segment file (of the form segment_partition_xxx_from_offset_xxxxxx_records)";
    private static final String CMD_SHOW = "show";
    private static final String ARG_OFFSET = "offset";
    private static final String ARG_OFFSET_HELP = "The offset of the message to display";
    private static final String CMD_LIST_HELP = "Lists all records in the segment. Just counting the value length – not displaying it.";
    private static final String CMD_SHOW_HELP = "Shows a specific record in the segment. Displays key and value";
    private static final String[] COMMANDS = {CMD_LIST, CMD_SHOW};
    private static final String ARG_KEY_FORMAT = "key-formatter";
    private static final String ARG_KEY_FORMAT_HELP = "Which formatter to use to display the key (default: StringFormatter)";
    private static final String ARG_VALUE_FORMAT = "value-formatter";
    private static final String ARG_VALUE_FORMAT_HELP = "Which formatter to use to display the value (default: StringFormatter)";

    public static void main(String[] args) throws IOException {
        /*
        cli --list --segment [file]
        cli --show --segment [file] --offset 0
         */
        final OptionParser optionParser = new OptionParser();
        optionParser.accepts(ARG_SEGMENT, ARG_SEGMENT_HELP).withRequiredArg().ofType(String.class);
        optionParser.accepts(CMD_LIST, CMD_LIST_HELP);
        optionParser.accepts(CMD_SHOW, CMD_SHOW_HELP);
        optionParser.accepts(ARG_OFFSET, ARG_OFFSET_HELP).requiredIf(CMD_SHOW).withRequiredArg().ofType(Long.class);
        optionParser.accepts(ARG_KEY_FORMAT, ARG_KEY_FORMAT_HELP).withRequiredArg().ofType(String.class)
                .defaultsTo(RawFormatter.class.getCanonicalName());
        optionParser.accepts(ARG_VALUE_FORMAT, ARG_VALUE_FORMAT_HELP).withRequiredArg().ofType(String.class)
                .defaultsTo(RawFormatter.class.getCanonicalName());


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
        UnverifiedSegmentReader segmentReader = new UnverifiedSegmentReader(Paths.get(segmentIndexFileName));

        ByteFormatter keyFormatter = (ByteFormatter) instanciateClass((String) options.valueOf(ARG_KEY_FORMAT));
        ByteFormatter valueFormatter = (ByteFormatter) instanciateClass((String) options.valueOf(ARG_VALUE_FORMAT));
        if (options.has(CMD_LIST)) {
            RecordFormatter formatter = new ListRecordFormatter(keyFormatter, valueFormatter);
            list(segmentReader, formatter);
        } else if (options.has(CMD_SHOW)) {
            RecordFormatter formatter = new DetailedRecordFormatter(keyFormatter, valueFormatter);
            show(segmentReader, formatter, (Long) options.valueOf(ARG_OFFSET));
        }

    }

    private static Object instanciateClass(String name) {
        try {
            Class formatterClass = Class.forName(name);
            return formatterClass.getDeclaredConstructor().newInstance();
        } catch (ClassNotFoundException | IllegalAccessException | InstantiationException | NoSuchMethodException | InvocationTargetException e) {
            System.err.println("formatter must be a valid class");
            Exit.exit(1);
            // impossible to reach
            throw new RuntimeException("…");
        }
    }

    private static void show(UnverifiedSegmentReader segmentReader, RecordFormatter formatter, Long offset) {
        long maxOffset = -1;
        while (true) {
            try {
                Record record = segmentReader.read();
                maxOffset = record.kafkaOffset();
                if (record.kafkaOffset() == offset) {
                    formatter.writeTo(record, System.out);
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

    private static void list(UnverifiedSegmentReader segmentReader, RecordFormatter formatter) {
        int cnt = 0;
        while (true) {
            try {
                Record record = segmentReader.read();
                formatter.writeTo(record, System.out);
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
