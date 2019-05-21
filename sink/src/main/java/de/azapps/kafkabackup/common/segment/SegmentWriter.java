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
    private String filePrefix;
    private SegmentIndex segmentIndex;
    private FileOutputStream recordOutputStream;

    public SegmentWriter(String topic, int partition, long startOffset, Path topicDirectory) throws IOException, SegmentIndex.IndexException {
        this.topic = topic;
        this.partition = partition;
        filePrefix = topic + "_" + partition + "_" + startOffset;

        File indexFile = new File(topicDirectory.toFile(), filePrefix + "_index");
        File recordFile = new File(topicDirectory.toFile(), filePrefix + "_records");
        if (!indexFile.exists()) {
            indexFile.createNewFile();
        }
        if (!recordFile.exists()) {
            recordFile.createNewFile();
        }
        segmentIndex = new SegmentIndex(indexFile);
        recordOutputStream = new FileOutputStream(recordFile, true);
    }

    public SegmentIndex getSegmentIndex() {
        return segmentIndex;
    }

    public void append(Record record) throws IOException, SegmentIndex.IndexException {
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
                throw new SegmentIndex.IndexException("Offsets must be always increasing! There is something terribly wrong in your segmentIndex!");
            }
            startPosition = previousSegmentIndexEntry.getRecordFileOffset() + previousSegmentIndexEntry.getRecordByteLength();
        } else {
            startPosition = 0;
        }

        recordOutputStream.getChannel().position(startPosition);
        RecordSerde.write(recordOutputStream, record);
        long recordByteLength = recordOutputStream.getChannel().position() - startPosition;
        SegmentIndexEntry segmentIndexEntry = new SegmentIndexEntry(record.kafkaOffset(), startPosition, recordByteLength);
        segmentIndex.addEntry(segmentIndexEntry);
    }

    public String fileName() {
        return filePrefix;
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
}
