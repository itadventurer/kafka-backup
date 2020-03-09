package de.azapps.kafkabackup.common.segment;

import de.azapps.kafkabackup.common.TestUtils;
import de.azapps.kafkabackup.common.record.Record;
import de.azapps.kafkabackup.common.record.RecordSerde;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.OutputStream;
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
    private static Path TEMP_DIR = TestUtils.getTestDir("SegmentIndexTest");
    private static final SegmentIndexEntry ENTRY1 = new SegmentIndexEntry(0, 1, 10);
    private static final SegmentIndexEntry ENTRY2 = new SegmentIndexEntry(1, 11, 10);
    private static final SegmentIndexEntry ENTRY3 = new SegmentIndexEntry(5, 21, 15);
    private static final SegmentIndexEntry ENTRY4 = new SegmentIndexEntry(6, 36, 10);

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
        assertEquals(a.index(),b.index());
    }
    // Incrementing offsets
    @Test
    public void incrementingIndex() throws Exception {
        String indexFile = "incrementingIndex";
        SegmentIndex index = new SegmentIndex(Paths.get(TEMP_DIR.toString(), indexFile));
        List<SegmentIndexEntry> entries = new ArrayList<>();
        index.addEntry(new SegmentIndexEntry(5, 22, 15));
        // Wrong offset
        assertThrows(SegmentIndex.IndexException.class,
                () -> index.addEntry(new SegmentIndexEntry(0, 37, 10)));
        // Should be ok
        assertDoesNotThrow(() -> index.addEntry(new SegmentIndexEntry(10, 37, 10)));
        index.close();
    }
    // Read V1 Index
    // Find next entry if offset does not exist
    // Empty index
    // Errors in offsets, file positions

}
