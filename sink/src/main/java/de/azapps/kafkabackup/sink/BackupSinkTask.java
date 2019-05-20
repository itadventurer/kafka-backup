package de.azapps.kafkabackup.sink;

import de.azapps.kafkabackup.common.Constants;
import de.azapps.kafkabackup.common.Index;
import de.azapps.kafkabackup.common.PartitionWriter;
import de.azapps.kafkabackup.common.Record;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class BackupSinkTask extends SinkTask {
	private static final String TARGET_DIR_CONFIG = "target.dir";
	private Path targetDir;
	private Map<TopicPartition, PartitionWriter> partitionWriters = new HashMap<>();

	@Override
	public String version() {
		return Constants.VERSION;
	}

	@Override
	public void start(Map<String, String> props) {
		try {
			targetDir = Paths.get(props.get(TARGET_DIR_CONFIG));
			Files.createDirectories(targetDir);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void open(Collection<TopicPartition> partitions) {
		try {
			for (TopicPartition topicPartition : partitions) {
				PartitionWriter partitionWriter = new PartitionWriter(topicPartition.topic(), topicPartition.partition(), targetDir);
				partitionWriters.put(topicPartition, partitionWriter);
			}
		} catch (Index.IndexException e) {
			throw new RuntimeException("IndexException", e);
		} catch (IOException e) {
			throw new RuntimeException("IOException", e);
		}

	}

	@Override
	public void put(Collection<SinkRecord> records) {
		try {
			for (SinkRecord sinkRecord : records) {
				TopicPartition topicPartition = new TopicPartition(sinkRecord.topic(), sinkRecord.kafkaPartition());
				PartitionWriter partitionWriter = partitionWriters.get(topicPartition);
				partitionWriter.append(Record.fromSinkRecord(sinkRecord));
			}
		} catch (IOException | Index.IndexException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void stop() {
		try {
			for (PartitionWriter partitionWriter : partitionWriters.values()) {
				partitionWriter.close();
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
		try {
			for (PartitionWriter partitionWriter : partitionWriters.values()) {
				partitionWriter.flush();
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

}
