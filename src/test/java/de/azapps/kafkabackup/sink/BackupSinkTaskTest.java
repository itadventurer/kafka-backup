package de.azapps.kafkabackup.sink;

import de.azapps.kafkabackup.common.TestUtils;
import de.azapps.kafkabackup.common.partition.PartitionReader;
import de.azapps.kafkabackup.common.partition.PartitionWriter;
import de.azapps.kafkabackup.common.record.Record;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

public class BackupSinkTaskTest {
    private static final String TOPIC1 = "test-topic1";
    private static final String TOPIC2 = "test-topic2";
    private static final String TOPIC3 = "test-topic3";
    private static final byte[] KEY_BYTES = "test-key".getBytes(StandardCharsets.UTF_8);
    private static final byte[] VALUE_BYTES = "test-value".getBytes(StandardCharsets.UTF_8);
    private static final Headers HEADERS = new ConnectHeaders();
    private static Path TEMP_DIR = TestUtils.getTestDir("BackupSinkTaskTest");
    private static final Map<String, String> DEFAULT_PROPS = new HashMap<>();

    static {
        HEADERS.add("", new SchemaAndValue(Schema.OPTIONAL_BYTES_SCHEMA, new byte[0]));
        HEADERS.add("null", new SchemaAndValue(Schema.OPTIONAL_BYTES_SCHEMA, null));
        HEADERS.add("value", new SchemaAndValue(Schema.OPTIONAL_BYTES_SCHEMA, VALUE_BYTES));

        DEFAULT_PROPS.put(BackupSinkConfig.CLUSTER_BOOTSTRAP_SERVERS, "");
        DEFAULT_PROPS.put(BackupSinkConfig.MAX_SEGMENT_SIZE, String.valueOf(100));
    }

    @Test
    public void simpleTest() throws Exception {
        // Prepare
        Path directory = Paths.get(TEMP_DIR.toString(), "simpleTest");
        Files.createDirectories(directory);
        Map<String, String> props = new HashMap<>(DEFAULT_PROPS);
        props.put(BackupSinkConfig.TARGET_DIR_CONFIG, directory.toString());

        List<Record> records = new ArrayList<>();
        records.add(new Record(TOPIC1, 0, KEY_BYTES, VALUE_BYTES, 0));
        records.add(new Record(TOPIC1, 0, null, null, 1));
        records.add(new Record(TOPIC1, 0, KEY_BYTES, VALUE_BYTES, 10));

        List<TopicPartition> partitions = new ArrayList<>();
        partitions.add(new TopicPartition(TOPIC1, 0));

        // Start Task
        BackupSinkTask task = new BackupSinkTask();
        task.start(props, new MockOffsetSink(null, null), null, (n) -> {});
        task.open(partitions);
        task.put(records.stream().map(Record::toSinkRecord).collect(Collectors.toList()));

        // Write again
        List<Record> records2 = new ArrayList<>();
        records2.add(new Record(TOPIC1, 0, KEY_BYTES, VALUE_BYTES, 11));
        records2.add(new Record(TOPIC1, 0, null, null, 12));
        records2.add(new Record(TOPIC1, 0, KEY_BYTES, VALUE_BYTES, 13));
        task.put(records2.stream().map(Record::toSinkRecord).collect(Collectors.toList()));

        task.close(partitions);

        // Check backed up data
        List<Record> allRecords = new ArrayList<>(records);
        allRecords.addAll(records2);
        PartitionReader partitionReader = new PartitionReader(TOPIC1, 0, Paths.get(directory.toString(), TOPIC1));
        assertEquals(allRecords, partitionReader.readFully());
    }

    @Test
    public void simpleTestWithSnapshotMode() throws Exception {
        // Prepare
        Path directory = Paths.get(TEMP_DIR.toString(), "simpleTestWithSnapshotMode");
        Files.createDirectories(directory);
        Map<String, String> props = new HashMap<>(DEFAULT_PROPS);
        props.put(BackupSinkConfig.TARGET_DIR_CONFIG, directory.toString());
        props.put(BackupSinkConfig.SNAPSHOT, "true");

        List<Record> records = new ArrayList<>();
        Map<TopicPartition, Long> endOffsets = new HashMap<>();
        endOffsets.put(new TopicPartition(TOPIC1, 0), 13L);

        records.add(new Record(TOPIC1, 0, KEY_BYTES, VALUE_BYTES, 0));
        records.add(new Record(TOPIC1, 0, null, null, 1));
        records.add(new Record(TOPIC1, 0, KEY_BYTES, VALUE_BYTES, 10));

        List<TopicPartition> partitions = new ArrayList<>();
        partitions.add(new TopicPartition(TOPIC1, 0));

        AtomicBoolean endConditionCheck = new AtomicBoolean();
        // Start Task
        BackupSinkTask task = new BackupSinkTask();
        task.initialize(new MockSinkTaskContext());
        task.start(props, new MockOffsetSink(null, null), new MockEndOffsetReader(endOffsets), (n) -> endConditionCheck.set(true));

        task.open(partitions);
        task.put(records.stream().map(Record::toSinkRecord).collect(Collectors.toList()));

        assertFalse(endConditionCheck.get());

        // Write again
        List<Record> records2 = new ArrayList<>();
        records2.add(new Record(TOPIC1, 0, KEY_BYTES, VALUE_BYTES, 11));
        records2.add(new Record(TOPIC1, 0, null, null, 12));
        records2.add(new Record(TOPIC1, 0, KEY_BYTES, VALUE_BYTES, 13));
        task.put(records2.stream().map(Record::toSinkRecord).collect(Collectors.toList()));

        task.close(partitions);

        // Check backed up data
        List<Record> allRecords = new ArrayList<>(records);
        allRecords.addAll(records2);
        PartitionReader partitionReader = new PartitionReader(TOPIC1, 0, Paths.get(directory.toString(), TOPIC1));
        assertEquals(allRecords, partitionReader.readFully());
        assertTrue(endConditionCheck.get());
    }

    @Test
    public void multipleTopicPartitionsTest() throws Exception {
        // Prepare
        Path directory = Paths.get(TEMP_DIR.toString(), "multipleTopicPartitionsTest");
        Files.createDirectories(directory);
        Map<String, String> props = new HashMap<>(DEFAULT_PROPS);
        props.put(BackupSinkConfig.TARGET_DIR_CONFIG, directory.toString());

        List<Record> records = new ArrayList<>();
        records.add(new Record(TOPIC1, 0, KEY_BYTES, VALUE_BYTES, 0));  //T1 P0
        records.add(new Record(TOPIC2, 0, KEY_BYTES, VALUE_BYTES, 0));  //T2 P0
        records.add(new Record(TOPIC1, 1, null, null, 1));   //T1 P1
        records.add(new Record(TOPIC2, 1, null, null, 1));   //T2 P1
        records.add(new Record(TOPIC1, 2, KEY_BYTES, VALUE_BYTES, 10)); //T1 P2
        records.add(new Record(TOPIC3, 1, null, null, 1));   //T3 P1

        List<TopicPartition> partitions = new ArrayList<>();
        partitions.add(new TopicPartition(TOPIC1, 0));
        partitions.add(new TopicPartition(TOPIC1, 1));
        partitions.add(new TopicPartition(TOPIC1, 2));
        partitions.add(new TopicPartition(TOPIC2, 0));
        partitions.add(new TopicPartition(TOPIC2, 1));
        partitions.add(new TopicPartition(TOPIC2, 2));
        partitions.add(new TopicPartition(TOPIC3, 0));
        partitions.add(new TopicPartition(TOPIC3, 1));
        partitions.add(new TopicPartition(TOPIC3, 2));

        // Start Task
        BackupSinkTask task = new BackupSinkTask();
        task.start(props, new MockOffsetSink(null, null), null, (n) -> {});
        task.open(partitions);
        task.put(records.stream().map(Record::toSinkRecord).collect(Collectors.toList()));
        task.close(partitions);

        // Check backed up data

        //T1 P0
        List<Record> t1p0Records = new ArrayList<>();
        t1p0Records.add(records.get(0));
        PartitionReader t1p0Reader = new PartitionReader(TOPIC1, 0, Paths.get(directory.toString(), TOPIC1));
        assertEquals(t1p0Records, t1p0Reader.readFully());
        //T2 P0
        List<Record> t2p0Records = new ArrayList<>();
        t2p0Records.add(records.get(1));
        PartitionReader t2p0Reader = new PartitionReader(TOPIC2, 0, Paths.get(directory.toString(), TOPIC2));
        assertEquals(t2p0Records, t2p0Reader.readFully());
        //T1 P1
        List<Record> t1p1Records = new ArrayList<>();
        t1p1Records.add(records.get(2));
        PartitionReader t1p1Reader = new PartitionReader(TOPIC1, 1, Paths.get(directory.toString(), TOPIC1));
        assertEquals(t1p1Records, t1p1Reader.readFully());
        //T2 P1
        List<Record> t2p1Records = new ArrayList<>();
        t2p1Records.add(records.get(3));
        PartitionReader t2p1Reader = new PartitionReader(TOPIC2, 1, Paths.get(directory.toString(), TOPIC2));
        assertEquals(t2p1Records, t2p1Reader.readFully());
        //T1 P2
        List<Record> t1p2Records = new ArrayList<>();
        t1p2Records.add(records.get(4));
        PartitionReader t1p2Reader = new PartitionReader(TOPIC1, 2, Paths.get(directory.toString(), TOPIC1));
        assertEquals(t1p2Records, t1p2Reader.readFully());
        //T3 P1
        List<Record> t3p1Records = new ArrayList<>();
        t3p1Records.add(records.get(5));
        PartitionReader t3p1Reader = new PartitionReader(TOPIC3, 1, Paths.get(directory.toString(), TOPIC3));
        assertEquals(t3p1Records, t3p1Reader.readFully());
    }

    @Test
    public void invalidOffsetsTest() throws Exception {
        // Prepare
        Path directory = Paths.get(TEMP_DIR.toString(), "invalidOffsetsTest");
        Files.createDirectories(directory);
        Map<String, String> props = new HashMap<>(DEFAULT_PROPS);
        props.put(BackupSinkConfig.TARGET_DIR_CONFIG, directory.toString());

        // Invalid offsets
        List<Record> records = new ArrayList<>();
        records.add(new Record(TOPIC1, 0, KEY_BYTES, VALUE_BYTES, 0));
        records.add(new Record(TOPIC1, 0, null, null, 10));
        records.add(new Record(TOPIC1, 0, KEY_BYTES, VALUE_BYTES, 9)); // invalid offset!
        records.add(new Record(TOPIC1, 0, KEY_BYTES, VALUE_BYTES, 11));

        // Some more (valid) data
        List<Record> records2 = new ArrayList<>();
        records2.add(new Record(TOPIC1, 0, KEY_BYTES, VALUE_BYTES, 11));
        records2.add(new Record(TOPIC1, 0, KEY_BYTES, VALUE_BYTES, 12));

        List<TopicPartition> partitions = new ArrayList<>();
        partitions.add(new TopicPartition(TOPIC1, 0));

        // Start Task
        BackupSinkTask task = new BackupSinkTask();
        task.start(props, new MockOffsetSink(null, null), null, (n) -> {});
        task.open(partitions);
        assertThrows(RuntimeException.class, () -> task.put(records.stream().map(Record::toSinkRecord).collect(Collectors.toList())));
        assertDoesNotThrow(() -> task.put(records2.stream().map(Record::toSinkRecord).collect(Collectors.toList())));
        task.close(partitions);

        List<Record> expectedRecords = records.subList(0, 2);
        expectedRecords.addAll(records2);

        // Data written until the invalid offset should stay there
        PartitionReader partitionReader = new PartitionReader(TOPIC1, 0, Paths.get(directory.toString(), TOPIC1));
        assertEquals(expectedRecords, partitionReader.readFully());
    }

    @Test
    public void writeToExistingData() throws Exception {
        // Prepare initial data
        Path directory = Paths.get(TEMP_DIR.toString(), "writeToExistingData");
        Files.createDirectories(directory);

        List<Record> initialRecords = new ArrayList<>();
        initialRecords.add(new Record(TOPIC1, 0, KEY_BYTES, VALUE_BYTES, 0));
        initialRecords.add(new Record(TOPIC1, 0, null, null, 1));
        initialRecords.add(new Record(TOPIC1, 0, new byte[0], new byte[0], 2));

        PartitionWriter partitionWriter = new PartitionWriter(TOPIC1, 0, Paths.get(directory.toString(), TOPIC1), 100);
        for (Record record : initialRecords) {
            partitionWriter.append(record);
        }
        partitionWriter.close();

        // Writer

        Map<String, String> props = new HashMap<>(DEFAULT_PROPS);
        props.put(BackupSinkConfig.TARGET_DIR_CONFIG, directory.toString());

        List<Record> records = new ArrayList<>();
        records.add(new Record(TOPIC1, 0, KEY_BYTES, VALUE_BYTES, 3));
        records.add(new Record(TOPIC1, 0, null, null, 4));
        records.add(new Record(TOPIC1, 0, KEY_BYTES, VALUE_BYTES, 5));

        List<TopicPartition> partitions = new ArrayList<>();
        partitions.add(new TopicPartition(TOPIC1, 0));

        // Start Task
        BackupSinkTask task = new BackupSinkTask();
        task.initialize(new MockSinkTaskContext());
        task.start(props, new MockOffsetSink(null, null), null, (n) -> {});
        task.open(partitions);
        task.put(records.stream().map(Record::toSinkRecord).collect(Collectors.toList()));
        task.close(partitions);

        // Check backed up data
        List<Record> allRecords = new ArrayList<>(initialRecords);
        allRecords.addAll(records);
        PartitionReader partitionReader = new PartitionReader(TOPIC1, 0, Paths.get(directory.toString(), TOPIC1));
        assertEquals(allRecords, partitionReader.readFully());
    }
}
