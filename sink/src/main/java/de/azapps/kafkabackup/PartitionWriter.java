package de.azapps.kafkabackup;

import org.apache.kafka.connect.sink.SinkRecord;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class PartitionWriter {
	private String topic;
	private int partition;
	private String filePrefix;
	private Index index;
	private FileOutputStream recordOutputStream;
	private FileInputStream recordInputStream;
	private int readerPosition = 0;

	public PartitionWriter(String topic, int partition, Path directory) throws IOException, Index.IndexException {
		this.topic = topic;
		this.partition = partition;
		filePrefix = topic + "_" + partition;

		File indexFile = new File(directory.toFile(), filePrefix + "_index");
		File recordFile = new File(directory.toFile(), filePrefix);
		if (!indexFile.exists()) {
			indexFile.createNewFile();
		}
		if (!recordFile.exists()) {
			recordFile.createNewFile();
		}
		index = new Index(indexFile);
		recordOutputStream = new FileOutputStream(recordFile, true);
		recordInputStream = new FileInputStream(recordFile);
	}

	public Index getIndex() {
		return index;
	}

	public void append(Record record) throws IOException, Index.IndexException {
		if (!record.topic().equals(topic) || record.kafkaPartition() != partition) {
			throw new RuntimeException("Trying to append to wrong topic or partition!\n" +
				"Expected topic: " + topic + " given topic: " + record.topic() + "\n" +
				"Expected partition: " + partition + " given partition: " + partition);
		}
		Optional<IndexEntry> optionalPreviousIndexEntry = index.lastIndexEntry();
		long startPosition;
		if (optionalPreviousIndexEntry.isPresent()) {
			IndexEntry previousIndexEntry = optionalPreviousIndexEntry.get();

			if (record.kafkaOffset() <= previousIndexEntry.getOffset()) {
				throw new Index.IndexException("Offsets must be always increasing! There is something terribly wrong in your index!");
			}
			startPosition = previousIndexEntry.getRecordFileOffset() + previousIndexEntry.getRecordByteLength();
		} else {
			startPosition = 0;
		}

		recordOutputStream.getChannel().position(startPosition);
		RecordSerde.write(recordOutputStream, record);
		long recordByteLength = recordOutputStream.getChannel().position() - startPosition;
		IndexEntry indexEntry = new IndexEntry(record.kafkaOffset(), startPosition, recordByteLength);
		index.addEntry(indexEntry);
	}

	public Optional<Record> readNext() throws IOException {
		Optional<IndexEntry> currentRecordIndex = index.getByPosition(readerPosition);
		if(currentRecordIndex.isEmpty()) {
			return Optional.empty();
		} else {
			recordInputStream.getChannel().position(currentRecordIndex.get().getRecordFileOffset());
			Record currentRecord = RecordSerde.read(topic, partition, recordInputStream);
			readerPosition++;
			return Optional.of(currentRecord);
		}
	}

	public List<Record> readAll() throws IOException {
		List<Record> records = new ArrayList<>(index.size());
		readerPosition=0;
		while(true) {
			Optional<Record> optionalRecord = readNext();
			if(optionalRecord.isPresent()) {
				records.add(optionalRecord.get());
			} else {
				return records;
			}
		}
	}

	public void flush() throws IOException {
		recordOutputStream.flush();
		index.flush();
	}

	public void close() throws IOException {
		recordOutputStream.close();
		index.close();
	}
}
