package de.azapps.kafkabackup.common.segment;

import de.azapps.kafkabackup.common.record.Record;

import java.io.EOFException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class SegmentIndexRestore {
    private final SegmentIndex segmentIndex;
    private final UnverifiedSegmentReader reader;

    public SegmentIndexRestore(Path segmentFile) throws IOException, RestoreException, SegmentIndex.IndexException {
        int partition = SegmentUtils.getPartitionFromSegment(segmentFile);
        long startOffset = SegmentUtils.getStartOffsetFromSegment(segmentFile);
        Path parent = segmentFile.toAbsolutePath().getParent();
        if (parent == null) {
            throw new RestoreException("Segment file " + segmentFile + " does not exist");
        }
        Path indexFile = SegmentUtils.indexFile(parent, partition, startOffset);

        if (!Files.isRegularFile(segmentFile)) {
            throw new RestoreException("Segment file " + segmentFile + " does not exist");
        }
        if (Files.isRegularFile(indexFile)) {
            throw new RestoreException("Index file " + indexFile + " must not exist");
        }
        segmentIndex = new SegmentIndex(indexFile);
        reader = new UnverifiedSegmentReader(segmentFile);
    }

    public void restore() throws IOException, SegmentIndex.IndexException {
        long lastPosition = 1; // mind the magic byte!
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
