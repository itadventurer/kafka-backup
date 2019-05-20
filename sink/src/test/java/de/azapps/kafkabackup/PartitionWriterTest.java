package de.azapps.kafkabackup;

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class PartitionWriterTest {

	private String tmpDirPrefix = "PartitionWriterTest";
	private Path tmpDir;


	public PartitionWriterTest() {
		String tempDir = System.getProperty("java.io.tmpdir");
		this.tmpDir = Paths.get(tempDir, tmpDirPrefix);
	}

	@Before
	public void setUp() throws Exception {
		if (Files.exists(tmpDir)) {
			Files.walk(tmpDir)
				.sorted(Comparator.reverseOrder())
				.map(Path::toFile)
				.forEach(File::delete);
		}
		Files.createDirectories(tmpDir);
	}

	private Record genRecord(String topic, int partition, long offset) {
		String val = "t:" + topic + " p:" + partition + " o:" + offset;
		byte[] key = ("key of: " + val).getBytes();
		byte[] value = ("value of: " + val).getBytes();
		return new Record(topic, partition, key, value, offset);
	}

	@Test
	public void simpleTest() throws Exception {
		String topic = "foo";
		int partition = 0;
		PartitionWriter partitionWriter = new PartitionWriter(topic, partition, tmpDir);
		Index index = partitionWriter.getIndex();
		IndexEntry indexEntry = index.lastIndexEntry().orElse(null);
		long startOffset;
		if (indexEntry != null) {
			startOffset = indexEntry.getOffset() + 1;
		} else {
			startOffset = 0;
		}

		List<Record> writtenRecords = new ArrayList<>();

		for (long offset = startOffset; offset < startOffset + 3; offset++) {
			Record record = genRecord(topic, partition, offset);
			partitionWriter.append(record);
			writtenRecords.add(record);
		}
		partitionWriter.close();

		List<Record> readRecords = partitionWriter.readAll();
		assertEquals(writtenRecords, readRecords);
	}

}
