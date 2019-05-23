package de.azapps.kafkabackup.common.segment;

import de.azapps.kafkabackup.common.record.Record;
import de.azapps.kafkabackup.common.record.RecordSerde;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;

public class SegmentWriter {
    private String topic;
    private int partition;
    private long startOffset;
    private SegmentIndex segmentIndex;
    private FileOutputStream recordOutputStream;
    private Path topicDir;

    public SegmentWriter(String topic, int partition, long startOffset, Path topicDir) throws IOException, SegmentIndex.IndexException {
        this.topic = topic;
        this.partition = partition;
        this.topicDir = topicDir;
        this.startOffset = startOffset;

        File indexFile = SegmentUtils.indexFile(topicDir, partition, startOffset);
        File recordFile = SegmentUtils.recordsFile(topicDir, partition, startOffset);
        if (!indexFile.exists()) {
            indexFile.createNewFile();
        }
        if (!recordFile.exists()) {
            recordFile.createNewFile();
        }
        segmentIndex = new SegmentIndex(indexFile);
        recordOutputStream = new FileOutputStream(recordFile, true);
    }



    public void append(Record record) throws IOException, SegmentIndex.IndexException, SegmentException {
        if (!record.topic().equals(topic) || record.kafkaPartition() != partition) {
            throw new RuntimeException("Trying to append to wrong topic or partition!\n" +
                    "Expected topic: " + topic + " given topic: " + record.topic() + "\n" +
                    "Expected partition: " + partition + " given partition: " + partition);
        }
        Optional<SegmentIndexEntry> optionalPreviousIndexEntry = segmentIndex.lastIndexEntry();
        long startPosition;
        if (optionalPreviousIndexEntry.isPresent()) {
            SegmentIndexEntry previousSegmentIndexEntry = optionalPreviousIndexEntry.get();

            if (record.kafkaOffset() <= previousSegmentIndexEntry.getOffset()) {
                // Handle at least once semantics: Read from partition file and verify
                // that the written data is the same as the one we want to write.
                // Otherwise we are overwriting some data (maybe from a previous backup) and should throw exceptions!
                SegmentReader segmentReader = new SegmentReader(topic, partition, topicDir, startOffset);
                segmentReader.seek(record.kafkaOffset());
                Record fsRecord = segmentReader.read();
                if(!record.equals(fsRecord)) {
                    throw new SegmentException("Trying to override a written record. Records not equal. There is something terribly wrong in your setup! Please check whether you are trying to override an existing backup");
                }
                return;
            }
            startPosition = previousSegmentIndexEntry.recordFilePosition() + previousSegmentIndexEntry.recordByteLength();
        } else {
            startPosition = 0;
        }

        recordOutputStream.getChannel().position(startPosition);
        RecordSerde.write(recordOutputStream, record);
        long recordByteLength = recordOutputStream.getChannel().position() - startPosition;
        SegmentIndexEntry segmentIndexEntry = new SegmentIndexEntry(record.kafkaOffset(), startPosition, recordByteLength);
        segmentIndex.addEntry(segmentIndexEntry);
    }

    public String filePrefix() {
        return SegmentUtils.filePrefix(partition, startOffset);
    }

    public long size() throws IOException {
        return recordOutputStream.getChannel().size();
    }

    public void flush() throws IOException {
        recordOutputStream.flush();
        segmentIndex.flush();
    }

    public void close() throws IOException {
        recordOutputStream.close();
        segmentIndex.close();
    }

    public static class SegmentException extends Exception {
        SegmentException(String message) {
            super(message);
        }
    }
}
