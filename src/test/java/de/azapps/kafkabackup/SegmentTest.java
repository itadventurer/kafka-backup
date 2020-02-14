package de.azapps.kafkabackup;

import de.azapps.kafkabackup.common.record.Record;

import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;
import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.util.Comparator;

public class SegmentTest {

	private String tmpDirPrefix = "SegmentTest";
	private Path tmpDir;


	public SegmentTest() {
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
		/*String topic = "foo";
		int partition = 0;
		Segment segment = new Segment(topic, partition, 0, tmpDir);
		SegmentIndex segmentIndex = segment.getSegmentIndex();
		SegmentIndexEntry segmentIndexEntry = segmentIndex.lastIndexEntry().orElse(null);
		long startOffset;
		if (segmentIndexEntry != null) {
			startOffset = segmentIndexEntry.getOffset() + 1;
		} else {
			startOffset = 0;
		}

		List<Record> writtenRecords = new ArrayList<>();

		for (long offset = startOffset; offset < startOffset + 3; offset++) {
			Record record = genRecord(topic, partition, offset);
			segment.append(record);
			writtenRecords.add(record);
		}
		segment.close();

		List<Record> readRecords = segment.readAll();
		assertEquals(writtenRecords, readRecords);
		 */
	}
}
