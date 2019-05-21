package de.azapps.kafkabackup.common.segment;

import de.azapps.kafkabackup.common.record.Record;
import de.azapps.kafkabackup.common.record.RecordSerde;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class SegmentReader {
    private String topic;
    private int partition;
    private SegmentIndex segmentIndex;
    private FileInputStream recordInputStream;
    private int readerPosition = 0;

    public SegmentReader(String topic, int partition, String filePrefix) throws IOException, SegmentIndex.IndexException {
        this.topic = topic;
        this.partition = partition;

        File indexFile = new File(filePrefix + "_index");
        File recordFile = new File(filePrefix + "_records");
        if (!indexFile.exists()) {
            throw new RuntimeException("Index for Segment not found: " + filePrefix);
        }
        if (!recordFile.exists()) {
            throw new RuntimeException("Segment not found: " + filePrefix);
        }
        segmentIndex = new SegmentIndex(indexFile);
        recordInputStream = new FileInputStream(recordFile);
    }

    public SegmentIndex getSegmentIndex() {
        return segmentIndex;
    }

    public Optional<Record> readNext() throws IOException {
        Optional<SegmentIndexEntry> currentRecordIndex = segmentIndex.getByPosition(readerPosition);
        if (currentRecordIndex.isEmpty()) {
            return Optional.empty();
        } else {
            recordInputStream.getChannel().position(currentRecordIndex.get().getRecordFileOffset());
            Record currentRecord = RecordSerde.read(topic, partition, recordInputStream);
            readerPosition++;
            return Optional.of(currentRecord);
        }
    }

    public List<Record> readAll() throws IOException {
        List<Record> records = new ArrayList<>(segmentIndex.size());
        readerPosition = 0;
        while (true) {
            Optional<Record> optionalRecord = readNext();
            if (optionalRecord.isPresent()) {
                records.add(optionalRecord.get());
            } else {
                return records;
            }
        }
    }

    public void close() throws IOException {
        recordInputStream.close();
        segmentIndex.close();
    }
}
