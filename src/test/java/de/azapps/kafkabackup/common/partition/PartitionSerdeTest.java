package de.azapps.kafkabackup.common.partition;

import de.azapps.kafkabackup.common.TestUtils;
import de.azapps.kafkabackup.common.record.Record;
import de.azapps.kafkabackup.common.segment.SegmentReader;
import de.azapps.kafkabackup.common.segment.SegmentUtils;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class PartitionSerdeTest {
    private static final String TOPIC = "test-topic";
    private static final byte[] KEY_BYTES = "test-key".getBytes(StandardCharsets.UTF_8);
    private static final byte[] VALUE_BYTES = "test-value".getBytes(StandardCharsets.UTF_8);
    private static final Path TEMP_DIR = TestUtils.getTestDir("PartitionSerdeTest");

    private static final RecordHeaders HEADERS = new RecordHeaders();
    private static final byte[] HEADER_0_VALUE_BYTES = "header0-value".getBytes(StandardCharsets.UTF_8);
    private static final byte[] HEADER_1_VALUE_BYTES = "header1-value".getBytes(StandardCharsets.UTF_8);
    static {
        HEADERS.add("", new byte[0]);
        HEADERS.add("null", null);
        HEADERS.add("value0", HEADER_0_VALUE_BYTES);
        HEADERS.add("value1", HEADER_1_VALUE_BYTES);
    }
    @Test
    public void simpleRoundtripTest() throws Exception {
        int partition = 0;

        List<Record> records = new ArrayList<>();
        records.add(new Record(TOPIC, partition, KEY_BYTES, VALUE_BYTES, 0));
        records.add(new Record(TOPIC, partition, null, null, 1));
        records.add(new Record(TOPIC, partition, new byte[0], new byte[0], 2));
        records.add(new Record(TOPIC, partition, KEY_BYTES, VALUE_BYTES, 3, null, TimestampType.NO_TIMESTAMP_TYPE, HEADERS));

        PartitionWriter partitionWriter = new PartitionWriter(TOPIC, partition, TEMP_DIR, 50);
        partitionWriter.append(records.get(0));
        partitionWriter.append(records.get(1));
        partitionWriter.append(records.get(2));
        partitionWriter.append(records.get(3));
        partitionWriter.close();

        PartitionReader partitionReader = new PartitionReader(TOPIC, partition, TEMP_DIR);
        assertEquals(records, partitionReader.readFully());
        assertFalse(partitionReader.hasMoreData());
        partitionReader.seek(1);
        assertEquals(records.get(1), partitionReader.read());
        partitionReader.seek(3);
        assertEquals(records.get(3), partitionReader.read());
        assertFalse(partitionReader.hasMoreData());
    }

    @Test
    public void smallSegmentSizeTest() throws Exception {
        int partition = 1;

        List<Record> records = new ArrayList<>();
        records.add(new Record(TOPIC, partition, KEY_BYTES, VALUE_BYTES, 0));
        records.add(new Record(TOPIC, partition, null, null, 1));
        records.add(new Record(TOPIC, partition, new byte[0], new byte[0], 2));

        PartitionWriter partitionWriter = new PartitionWriter(TOPIC, partition, TEMP_DIR, 1);
        partitionWriter.append(records.get(0));
        partitionWriter.append(records.get(1));
        partitionWriter.append(records.get(2));
        partitionWriter.close();

        SegmentReader a = new SegmentReader(TOPIC, partition, TEMP_DIR, 0);
        assertEquals(records.get(0), a.read());
        assertFalse(a.hasMoreData());
        SegmentReader b = new SegmentReader(TOPIC, partition, TEMP_DIR, 1);
        assertEquals(records.get(1), b.read());
        assertFalse(b.hasMoreData());
        SegmentReader c = new SegmentReader(TOPIC, partition, TEMP_DIR, 2);
        assertEquals(records.get(2), c.read());
        assertFalse(c.hasMoreData());
    }

    @Test
    public void deleteSomeSegmentsTest() throws Exception {
        int partition = 2;

        List<Record> records = new ArrayList<>();
        records.add(new Record(TOPIC, partition, KEY_BYTES, VALUE_BYTES, 0));
        records.add(new Record(TOPIC, partition, null, null, 1));
        records.add(new Record(TOPIC, partition, new byte[0], new byte[0], 2));
        records.add(new Record(TOPIC, partition, KEY_BYTES, VALUE_BYTES, 3, null, TimestampType.NO_TIMESTAMP_TYPE, HEADERS));

        PartitionWriter partitionWriter = new PartitionWriter(TOPIC, partition, TEMP_DIR, 1);
        partitionWriter.append(records.get(0));
        partitionWriter.append(records.get(1));
        partitionWriter.append(records.get(2));
        partitionWriter.append(records.get(3));
        partitionWriter.close();

        // Delete segments 0 and 2
        Files.delete(SegmentUtils.recordsFile(TEMP_DIR, partition, 0));
        Files.delete(SegmentUtils.indexFile(TEMP_DIR, partition, 0));
        Files.delete(SegmentUtils.recordsFile(TEMP_DIR, partition, 2));
        Files.delete(SegmentUtils.indexFile(TEMP_DIR, partition, 2));
        Files.delete(PartitionUtils.indexFile(TEMP_DIR, partition));

        // Restore indices
        PartitionIndexRestore restore = new PartitionIndexRestore(TEMP_DIR, partition);
        restore.restore();

        // Expected
        List<Record> expected = new ArrayList<>();
        expected.add(records.get(1));
        expected.add(records.get(3));

        PartitionReader reader = new PartitionReader(TOPIC, partition, TEMP_DIR);
        assertEquals(expected, reader.readFully());
    }
}
