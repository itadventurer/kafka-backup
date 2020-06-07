package de.azapps.kafkabackup.common.segment;

import de.azapps.kafkabackup.common.TestUtils;
import de.azapps.kafkabackup.common.record.Record;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

public class SegmentIndexTest {
    private static final String TOPIC = "test-topic";
    private static final byte[] KEY_BYTES = "test-key".getBytes(StandardCharsets.UTF_8);
    private static final byte[] VALUE_BYTES = "test-value".getBytes(StandardCharsets.UTF_8);
    private static final SegmentIndexEntry ENTRY1 = new SegmentIndexEntry(0, 1, 10);
    private static final SegmentIndexEntry ENTRY2 = new SegmentIndexEntry(1, 11, 10);
    private static final SegmentIndexEntry ENTRY3 = new SegmentIndexEntry(5, 21, 15);
    private static final SegmentIndexEntry ENTRY4 = new SegmentIndexEntry(6, 36, 10);
    private static final Path TEMP_DIR = TestUtils.getTestDir("SegmentIndexTest");

    @Test
    public void simpleRoundtripTest() throws Exception {
        String indexFile = "simpleRoundtripTestIndex";
        SegmentIndex index = new SegmentIndex(Paths.get(TEMP_DIR.toString(), indexFile));
        assertEquals(0L, index.lastValidStartPosition());
        List<SegmentIndexEntry> entries = new ArrayList<>();
        entries.add(ENTRY1);
        index.addEntry(ENTRY1);
        entries.add(ENTRY2);
        index.addEntry(ENTRY2);
        entries.add(ENTRY3);
        index.addEntry(ENTRY3);
        entries.add(ENTRY4);
        index.addEntry(ENTRY4);

        index.close();


        SegmentIndex b = new SegmentIndex(Paths.get(TEMP_DIR.toString(), indexFile));
        assertEquals(entries, b.index());
        assertEquals(Optional.of(ENTRY3.recordFilePosition()), b.findByOffset(5));
        assertEquals(Optional.of(ENTRY3.recordFilePosition()), b.findEarliestWithHigherOrEqualOffset(2));
        assertEquals(Optional.empty(), b.findEarliestWithHigherOrEqualOffset(11));
        assertEquals(36, b.lastValidStartPosition());
    }

    @Test
    public void writeRecordThenCheckIndex() throws Exception {
        int partition = 0;
        SegmentWriter segmentWriter = new SegmentWriter(TOPIC, partition, 0, TEMP_DIR);
        segmentWriter.append(new Record(TOPIC, partition, KEY_BYTES, VALUE_BYTES, 0));

        SegmentIndex i1 = new SegmentIndex(SegmentUtils.indexFile(TEMP_DIR, partition, 0));
        assertEquals(1, i1.size());


        segmentWriter.append(new Record(TOPIC, partition, null, null, 1));
        segmentWriter.append(new Record(TOPIC, partition, new byte[0], new byte[0], 2));
        segmentWriter.close();

        SegmentIndex i2 = new SegmentIndex(SegmentUtils.indexFile(TEMP_DIR, partition, 0));
        assertEquals(3, i2.size());
        long fileLength = SegmentUtils.recordsFile(TEMP_DIR, partition, 0).toFile().length();
        //noinspection OptionalGetWithoutIsPresent
        SegmentIndexEntry entry = i2.lastIndexEntry().get();
        assertEquals(fileLength, entry.recordFilePosition() + entry.recordByteLength());
    }

    @Test
    public void restoreTest() throws Exception {
        int partition = 1;
        List<Record> records = new ArrayList<>();
        records.add(new Record(TOPIC, partition, KEY_BYTES, VALUE_BYTES, 0));
        records.add(new Record(TOPIC, partition, null, null, 1));
        records.add(new Record(TOPIC, partition, new byte[0], new byte[0], 2));
        records.add(new Record(TOPIC, partition, KEY_BYTES, VALUE_BYTES, 3, null, TimestampType.NO_TIMESTAMP_TYPE));

        SegmentWriter segmentWriter = new SegmentWriter(TOPIC, partition, 0, TEMP_DIR);
        for (Record record : records) {
            segmentWriter.append(record);
        }
        segmentWriter.close();
        Path indexFile = SegmentUtils.indexFile(TEMP_DIR, partition, 0);
        SegmentIndex a = new SegmentIndex(indexFile);
        Files.delete(indexFile);
        SegmentIndexRestore restore = new SegmentIndexRestore(SegmentUtils.recordsFile(TEMP_DIR, partition, 0));
        restore.restore();
        SegmentIndex b = new SegmentIndex(indexFile);
        assertEquals(a.index(), b.index());
    }

    @Test
    public void incrementingIndex() throws Exception {
        String indexFile = "incrementingIndex";
        SegmentIndex index = new SegmentIndex(Paths.get(TEMP_DIR.toString(), indexFile));
        index.addEntry(new SegmentIndexEntry(5, 22, 15));
        // Wrong offset
        assertThrows(SegmentIndex.IndexException.class,
                () -> index.addEntry(new SegmentIndexEntry(0, 37, 10)));
        // Should be ok
        assertDoesNotThrow(() -> index.addEntry(new SegmentIndexEntry(10, 37, 10)));
        index.close();
    }

    @Test
    public void emptyIndexTest() throws Exception {
        String indexFile = "emptyIndexTest";
        SegmentIndex index = new SegmentIndex(Paths.get(TEMP_DIR.toString(), indexFile));
        assertEquals(0L, index.lastValidStartPosition());
        index.close();


        SegmentIndex b = new SegmentIndex(Paths.get(TEMP_DIR.toString(), indexFile));
        assertEquals(0L, b.lastValidStartPosition());
        assertEquals(Optional.empty(), b.findEarliestWithHigherOrEqualOffset(0));
        assertEquals(Optional.empty(), b.findEarliestWithHigherOrEqualOffset(11));
        assertTrue(b.index().isEmpty());
    }


    @Test
    public void testReadV1Index() throws Exception {
        String indexFile = "testIndex";
        Path directory = Paths.get("src/test/assets/v1/segmentindex");
        List<SegmentIndexEntry> entries = new ArrayList<>();
        entries.add(ENTRY1);
        entries.add(ENTRY2);
        entries.add(ENTRY3);
        entries.add(ENTRY4);

        SegmentIndex b = new SegmentIndex(Paths.get(directory.toString(), indexFile));
        assertEquals(entries, b.index());
        assertEquals(Optional.of(ENTRY3.recordFilePosition()), b.findByOffset(5));
        assertEquals(Optional.of(ENTRY3.recordFilePosition()), b.findEarliestWithHigherOrEqualOffset(2));
        assertEquals(36, b.lastValidStartPosition());
    }


    /**
     * Utility function to be run once when the format on disk changes to be able to stay backwards-compatible
     * <p>
     * Call it manually once when the format changes
     */
    private static void writeTestIndexToFile() throws Exception {
        String indexFile = "testIndex";
        Path directory = Paths.get("src/test/assets/v1/segmentindex"); // CHANGEME WHEN CHANGING DATA FORMAT!
        Files.createDirectories(directory);

        SegmentIndex index = new SegmentIndex(Paths.get(directory.toString(), indexFile));
        index.addEntry(ENTRY1);
        index.addEntry(ENTRY2);
        index.addEntry(ENTRY3);
        index.addEntry(ENTRY4);
        index.close();
    }
}
