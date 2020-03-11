package de.azapps.kafkabackup.common.segment;

import de.azapps.kafkabackup.common.record.Record;
import de.azapps.kafkabackup.common.record.RecordSerde;

import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class UnverifiedSegmentReader {
    private String topic;
    private int partition;
    private FileInputStream recordInputStream;

    public UnverifiedSegmentReader(Path recordFile) throws IOException {
        this(recordFile, "topic", 0);
    }

    public UnverifiedSegmentReader(Path recordFile, String topic, int partition) throws IOException {
        recordInputStream = new FileInputStream(recordFile.toFile());
        this.topic = topic;
        this.partition = partition;
        SegmentUtils.ensureValidSegment(recordInputStream);
    }

    public Record read() throws IOException {
        return RecordSerde.read(topic, partition, recordInputStream);
    }

    public List<Record> readN(int n) throws IOException {
        List<Record> records = new ArrayList<>(n);
        while (records.size() <= n) {
            try {
                Record record = read();
                records.add(record);
            } catch (EOFException e) {
                break;
            }
        }
        return records;
    }

    public List<Record> readFully() throws IOException {
        List<Record> records = new ArrayList<>();
        while (true) {
            try {
                Record record = read();
                records.add(record);
            } catch (EOFException e) {
                break;
            }
        }
        return records;
    }

    public long position() throws IOException {
        return recordInputStream.getChannel().position();
    }

    public void close() throws IOException {
        recordInputStream.close();
    }
}
