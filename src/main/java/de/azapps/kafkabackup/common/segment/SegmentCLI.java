package de.azapps.kafkabackup.common.segment;

import de.azapps.kafkabackup.common.record.Record;
import org.apache.kafka.common.utils.Exit;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * SegmentCLI
 * * list [file]
 * * show [file] [offset]
 */
public class SegmentCLI {
    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.out.println("Usage: SegmentCLI [commands]");
            System.out.println("Commands:");
            System.out.println("list [file]: Shows all records with offset, position in file and length");
            System.out.println("show [file] [offset]: Displays the key and value of a certain record");
            Exit.exit(-1);
        }

        String segmentIndexFileName = args[1];
        if (!segmentIndexFileName.endsWith("_records")) {
            segmentIndexFileName += "_records";
        }
        UnverifiedSegmentReader segmentReader = new UnverifiedSegmentReader(new File(segmentIndexFileName));

        switch (args[0]) {
            case "list":
                list(segmentReader);
                break;
            case "show":
                if (args.length != 3) {
                    System.out.println("Usage: SegmentCLI show [file] [offset]");
                    Exit.exit(-1);
                }
                show(segmentReader, Long.valueOf(args[2]));
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
