package de.azapps.kafkabackup.common.partition;

import de.azapps.kafkabackup.common.TestUtils;
import de.azapps.kafkabackup.common.segment.SegmentIndex;
import de.azapps.kafkabackup.common.segment.SegmentIndexEntry;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

public class PartitionIndexTest {
    private static Path TEMP_DIR = TestUtils.getTestDir("PartitionIndexTest");

    @Test
    public void simpleRoundtripTest() throws Exception {
        String indexFile = "simpleRoundtripTestIndex";
        List<PartitionIndexEntry> entries = new ArrayList<>();
        entries.add(new PartitionIndexEntry("s0", 0));
        entries.add(new PartitionIndexEntry("s100", 100));
        entries.add(new PartitionIndexEntry("s200", 200));
        entries.add(new PartitionIndexEntry("s300", 300));
        PartitionIndex index = new PartitionIndex(Paths.get(TEMP_DIR.toString(), indexFile));
        index.appendSegment(entries.get(0).filename(), entries.get(0).startOffset());
        index.appendSegment(entries.get(1).filename(), entries.get(1).startOffset());
        index.appendSegment(entries.get(2).filename(), entries.get(2).startOffset());
        index.appendSegment(entries.get(3).filename(), entries.get(3).startOffset());

        assertEquals(entries, index.index());
        index.close();


        PartitionIndex b = new PartitionIndex(Paths.get(TEMP_DIR.toString(), indexFile));
        assertEquals(entries, b.index());
        b.seek(10);
        assertEquals(entries.get(0).filename(), b.readFileName());
        assertTrue(b.hasMoreData());
        b.seek(200);
        assertEquals(entries.get(2).filename(), b.readFileName());
        assertTrue(b.hasMoreData());
        b.seek(310);
        assertEquals(entries.get(3).filename(), b.readFileName());
        assertFalse(b.hasMoreData());
        b.close();
    }


    @Test
    public void testReadV1Index() throws Exception {
        String indexFile = "testIndex";
        Path directory = Paths.get("src/test/assets/v1/partitionindex");
        List<PartitionIndexEntry> entries = new ArrayList<>();
        entries.add(new PartitionIndexEntry("s0", 0));
        entries.add(new PartitionIndexEntry("s100", 100));
        entries.add(new PartitionIndexEntry("s200", 200));
        entries.add(new PartitionIndexEntry("s300", 300));

        PartitionIndex b = new PartitionIndex(Paths.get(directory.toString(), indexFile));
        assertEquals(entries, b.index());
    }


    /**
     * Utility function to be run once when the format on disk changes to be able to stay backwards-compatible
     * <p>
     * Call it manually once when the format changes
     */
    private static void writeTestIndexToFile() throws Exception {
        String indexFile = "testIndex";
        Path directory = Paths.get("src/test/assets/v1/partitionindex"); // CHANGEME WHEN CHANGING DATA FORMAT!
        Files.createDirectories(directory);

        List<PartitionIndexEntry> entries = new ArrayList<>();
        entries.add(new PartitionIndexEntry("s0", 0));
        entries.add(new PartitionIndexEntry("s100", 100));
        entries.add(new PartitionIndexEntry("s200", 200));
        entries.add(new PartitionIndexEntry("s300", 300));

        PartitionIndex index = new PartitionIndex(Paths.get(directory.toString(), indexFile));
        index.appendSegment(entries.get(0).filename(), entries.get(0).startOffset());
        index.appendSegment(entries.get(1).filename(), entries.get(1).startOffset());
        index.appendSegment(entries.get(2).filename(), entries.get(2).startOffset());
        index.appendSegment(entries.get(3).filename(), entries.get(3).startOffset());
        index.close();
    }
}
