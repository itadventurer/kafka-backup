package de.azapps.kafkabackup.common.segment;

import de.azapps.kafkabackup.common.TestUtils;
import de.azapps.kafkabackup.common.record.Record;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;

import java.io.EOFException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class SegmentSerdeTest {
    private static final String TOPIC = "test-topic";
    private static final byte[] KEY_BYTES = "test-key".getBytes(StandardCharsets.UTF_8);
    private static final byte[] VALUE_BYTES = "test-value".getBytes(StandardCharsets.UTF_8);
    private static final RecordHeaders HEADERS = new RecordHeaders();
    private static final Path TEMP_DIR = TestUtils.getTestDir("SegmentSerdeTest");
    private static final byte[] NON_UTF8_BYTES = {0x012, 0x00, 0xf};

    static {
        HEADERS.add("", new byte[0]);
        HEADERS.add("null", null);
        HEADERS.add("value", VALUE_BYTES);
    }

    @Test
    public void simpleRoundtripTest() throws Exception {
        int partition = 0;

        List<Record> records = new ArrayList<>();
        records.add(new Record(TOPIC, partition, KEY_BYTES, VALUE_BYTES, 0));
        records.add(new Record(TOPIC, partition, null, null, 1));
        records.add(new Record(TOPIC, partition, new byte[0], new byte[0], 2));
        records.add(new Record(TOPIC, partition, KEY_BYTES, VALUE_BYTES, 3, null, TimestampType.NO_TIMESTAMP_TYPE, HEADERS));

        RecordHeaders headers = new RecordHeaders();
        headers.add("nonutf8", NON_UTF8_BYTES);
        records.add(new Record(TOPIC, partition, KEY_BYTES, VALUE_BYTES, 4, null, TimestampType.NO_TIMESTAMP_TYPE, headers));


        SegmentWriter segmentWriter = new SegmentWriter(TOPIC, partition, 0, TEMP_DIR);
        for (Record record : records) {
            segmentWriter.append(record);
        }
        assertEquals(segmentWriter.lastWrittenOffset(), 4);


        // Read with default reader
        SegmentReader segmentReader = new SegmentReader(TOPIC, partition, TEMP_DIR, 0);
        List<Record> b = segmentReader.readFully();
        assertEquals(records, b);

        // Read with unsafe reader
        Path segmentFile = SegmentUtils.recordsFile(TEMP_DIR, partition, 0);
        UnverifiedSegmentReader unverifiedSegmentReader = new UnverifiedSegmentReader(segmentFile, TOPIC, partition);
        List<Record> c = unverifiedSegmentReader.readFully();
        assertEquals(records, c);

        // No more records exist
        assertThrows(EOFException.class, segmentReader::read);
    }

    @Test
    public void offsetWriterErrors() throws Exception {
        int partition = 1;
        SegmentWriter segmentWriter = new SegmentWriter(TOPIC, partition, 10, TEMP_DIR);
        // Must not accept records with offsets smaller than the startOffset
        assertThrows(SegmentWriter.SegmentException.class, () ->
                segmentWriter.append(new Record(TOPIC, partition, KEY_BYTES, VALUE_BYTES, 5))
        );
        segmentWriter.append(new Record(TOPIC, partition, KEY_BYTES, VALUE_BYTES, 11));
        // Must not accept records with offsets smaller than the last offset
        assertThrows(SegmentWriter.SegmentException.class, () ->
                segmentWriter.append(new Record(TOPIC, partition, KEY_BYTES, VALUE_BYTES, 10))
        );
        // Must not accept duplicate offsets
        assertThrows(SegmentWriter.SegmentException.class, () ->
                segmentWriter.append(new Record(TOPIC, partition, KEY_BYTES, VALUE_BYTES, 11))
        );
    }

    @Test
    public void simpleWriterErrors() throws Exception {
        int partition = 2;
        SegmentWriter segmentWriter = new SegmentWriter(TOPIC, partition, 10, TEMP_DIR);
        // Must not accept wrong topic
        assertThrows(SegmentWriter.SegmentException.class, () ->
                segmentWriter.append(new Record("wrongTopic", partition, KEY_BYTES, VALUE_BYTES, 11))
        );
        // Must not accept wrong partition
        assertThrows(SegmentWriter.SegmentException.class, () ->
                segmentWriter.append(new Record(TOPIC, 0, KEY_BYTES, VALUE_BYTES, 11))
        );
    }

    @Test
    public void offsetGaps() throws Exception {
        int partition = 3;

        List<Record> records = new ArrayList<>();
        records.add(new Record(TOPIC, partition, KEY_BYTES, VALUE_BYTES, 0));
        records.add(new Record(TOPIC, partition, null, null, 1));
        records.add(new Record(TOPIC, partition, KEY_BYTES, VALUE_BYTES, 10));


        SegmentWriter segmentWriter = new SegmentWriter(TOPIC, partition, 0, TEMP_DIR);
        for (Record record : records) {
            segmentWriter.append(record);
        }
        assertEquals(segmentWriter.lastWrittenOffset(), 10);

        SegmentReader segmentReader = new SegmentReader(TOPIC, partition, TEMP_DIR, 0);
        assertEquals(records, segmentReader.readFully());
    }

    @Test
    public void emptySegments() throws Exception {
        int partition = 4;
        // It is ok if a segment is empty
        new SegmentWriter(TOPIC, partition, 0, TEMP_DIR);
        assertDoesNotThrow(() -> new SegmentReader(TOPIC, partition, TEMP_DIR, 0));
    }

    @Test
    public void playWithSeek() throws Exception {
        int partition = 5;

        List<Record> records = new ArrayList<>();
        records.add(new Record(TOPIC, partition, KEY_BYTES, VALUE_BYTES, 0));
        records.add(new Record(TOPIC, partition, null, null, 1));
        records.add(new Record(TOPIC, partition, KEY_BYTES, VALUE_BYTES, 10));


        SegmentWriter segmentWriter = new SegmentWriter(TOPIC, partition, 0, TEMP_DIR);
        for (Record record : records) {
            segmentWriter.append(record);
        }

        SegmentReader segmentReader = new SegmentReader(TOPIC, partition, TEMP_DIR, 0);
        assertEquals(records, segmentReader.readFully());

        // No more records exist
        assertThrows(EOFException.class, segmentReader::read);

        segmentReader.seek(0);
        assertEquals(records.get(0), segmentReader.read());
        segmentReader.seek(10);
        assertEquals(records.get(2), segmentReader.read());
        assertFalse(segmentReader.hasMoreData());

        segmentReader.seek(1);
        assertTrue(segmentReader.hasMoreData());
        assertEquals(records.get(1), segmentReader.read());
        assertEquals(records.get(2), segmentReader.read());
        assertFalse(segmentReader.hasMoreData());
        // Go to the next segment after a certain offset
        segmentReader.seek(6);
        assertEquals(records.get(2), segmentReader.read());
        segmentReader.seek(-1);
        assertEquals(records.get(0), segmentReader.read());
    }

    /**
     * DO NOT CHANGE THIS TEST!
     */
    @Test
    public void readV1Segment() throws Exception {
        int partition = 0;
        Path directory = Paths.get("src/test/assets/v1/segments");
        List<Record> records = new ArrayList<>();
        records.add(new Record(TOPIC, partition, KEY_BYTES, VALUE_BYTES, 0));
        records.add(new Record(TOPIC, partition, null, null, 1));
        records.add(new Record(TOPIC, partition, KEY_BYTES, VALUE_BYTES, 3, null, TimestampType.NO_TIMESTAMP_TYPE, HEADERS));
        records.add(new Record(TOPIC, partition, KEY_BYTES, VALUE_BYTES, 10));

        SegmentReader segmentReader = new SegmentReader(TOPIC, partition, directory, 0);
        assertEquals(records, segmentReader.readFully());
    }

    /**
     * Utility function to be run once when the format on disk changes to be able to stay backwards-compatible
     * <p>
     * Call it manually once when the format changes
     */
    private static void writeTestSegmentsToFile() throws Exception {
        int partition = 0;
        Path directory = Paths.get("src/test/assets/v1/segments"); // CHANGEME WHEN CHANGING DATA FORMAT!
        Files.createDirectories(directory);

        List<Record> records = new ArrayList<>();
        records.add(new Record(TOPIC, partition, KEY_BYTES, VALUE_BYTES, 0));
        records.add(new Record(TOPIC, partition, null, null, 1));
        records.add(new Record(TOPIC, partition, KEY_BYTES, VALUE_BYTES, 3, null, TimestampType.NO_TIMESTAMP_TYPE, HEADERS));
        records.add(new Record(TOPIC, partition, KEY_BYTES, VALUE_BYTES, 10));

        SegmentWriter segmentWriter = new SegmentWriter(TOPIC, partition, 0, directory);
        for (Record record : records) {
            segmentWriter.append(record);
        }
    }

}
