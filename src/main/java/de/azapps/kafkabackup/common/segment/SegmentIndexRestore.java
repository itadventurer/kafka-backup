package de.azapps.kafkabackup.common.segment;

import de.azapps.kafkabackup.common.record.Record;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class SegmentIndexRestore {
    private SegmentIndex segmentIndex;
    private UnverifiedSegmentReader reader;

    public SegmentIndexRestore(Path segmentFile) throws IOException, RestoreException, SegmentIndex.IndexException {
        int partition = SegmentUtils.getPartitionFromSegment(segmentFile);
        long startOffset = SegmentUtils.getStartOffsetFromSegment(segmentFile);
        File indexFile = SegmentUtils.indexFile(segmentFile.toAbsolutePath().getParent(), partition, startOffset);

        if (!Files.exists(segmentFile)) {
            throw new RestoreException("Segment file " + segmentFile + " does not exist");
        }
        if (indexFile.exists()) {
            throw new RestoreException("Index file " + indexFile + " must not exist");
        }
        indexFile.createNewFile();
        segmentIndex = new SegmentIndex(indexFile);
        reader = new UnverifiedSegmentReader(segmentFile.toFile());
    }

    public void restore() throws IOException, SegmentIndex.IndexException {
        long lastPosition = 0;
        while (true) {
            try {
                Record record = reader.read();
                long currentPosition = reader.position();
                SegmentIndexEntry indexEntry = new SegmentIndexEntry(record.kafkaOffset(), lastPosition, currentPosition - lastPosition);
                segmentIndex.addEntry(indexEntry);
                lastPosition = currentPosition;
            } catch (EOFException e) {
                break;
            }
        }
        segmentIndex.flush();
        segmentIndex.close();
    }

    public static class RestoreException extends Exception {
        RestoreException(String message) {
            super(message);
        }
    }


}
