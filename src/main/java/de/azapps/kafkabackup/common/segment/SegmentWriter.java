package de.azapps.kafkabackup.common.segment;

import de.azapps.kafkabackup.common.record.Record;
import de.azapps.kafkabackup.common.record.RecordBinarySerde;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
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

        Path indexFile = SegmentUtils.indexFile(topicDir, partition, startOffset);
        segmentIndex = new SegmentIndex(indexFile);

        Path recordFile = SegmentUtils.recordsFile(topicDir, partition, startOffset);
        if (!Files.isRegularFile(recordFile)) {
            Files.createFile(recordFile);
            recordOutputStream = new FileOutputStream(recordFile.toFile());
            recordOutputStream.write(SegmentUtils.V1_MAGIC_BYTE);
        } else {
            // Validate Magic Byte
            FileInputStream inputStream = new FileInputStream(recordFile.toFile());
            SegmentUtils.ensureValidSegment(inputStream);
            inputStream.close();

            // move to last committed position of the file
            recordOutputStream = new FileOutputStream(recordFile.toFile(), true);
            Optional<SegmentIndexEntry> optionalPreviousIndexEntry = segmentIndex.lastIndexEntry();
            if (optionalPreviousIndexEntry.isPresent()) {
                SegmentIndexEntry previousSegmentIndexEntry = optionalPreviousIndexEntry.get();
                long position = previousSegmentIndexEntry.recordFilePosition() + previousSegmentIndexEntry.recordByteLength();
                recordOutputStream.getChannel().position(position);
            } else {
                recordOutputStream.getChannel().position(1);
            }
        }
    }

    public long lastWrittenOffset(){
        return segmentIndex.lastIndexEntry().map(SegmentIndexEntry::getOffset).orElse(-1L);
    }

    public void append(Record record) throws IOException, SegmentIndex.IndexException, SegmentException {
        if (!record.topic().equals(topic)) {
            throw new RuntimeException("Trying to append to wrong topic!\n" +
                    "Expected topic: " + topic + " given topic: " + record.topic());
        }
        if(record.kafkaPartition() != partition) {
            throw new RuntimeException("Trying to append to wrong partition!\n" +
                    "Expected partition: " + partition + " given partition: " + partition);
        }
        if (record.kafkaOffset() <= lastWrittenOffset()) {
            // We are handling the offsets ourselves. This should never happen!
            throw new SegmentException("Trying to override a written record. There is something terribly wrong in your setup! Please check whether you are trying to override an existing backup");
        }
        long startPosition = recordOutputStream.getChannel().position();
        RecordBinarySerde.write(recordOutputStream, record);
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
