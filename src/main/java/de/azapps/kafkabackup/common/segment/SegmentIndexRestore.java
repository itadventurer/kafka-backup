package de.azapps.kafkabackup.common.segment;

import de.azapps.kafkabackup.common.record.Record;

import java.io.EOFException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class SegmentIndexRestore {
    private SegmentIndex segmentIndex;
    private UnverifiedSegmentReader reader;
    private Path segmentFile;
    private Path indexFile;

    public SegmentIndexRestore(Path segmentFile, Path indexFile) throws IOException, RestoreException, SegmentIndex.IndexException {
        this.segmentFile = segmentFile;
        this.indexFile = indexFile;

        if (!Files.exists(segmentFile)) {
            throw new RestoreException("Segment file " + segmentFile + " does not exist");
        }
        if (Files.exists(indexFile)) {
            throw new RestoreException("Index file " + indexFile + " must not exist");
        }
        Files.createFile(indexFile);
        segmentIndex = new SegmentIndex(indexFile.toFile());
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
