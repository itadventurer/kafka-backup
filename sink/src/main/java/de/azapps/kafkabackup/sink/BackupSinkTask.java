package de.azapps.kafkabackup.sink;

import de.azapps.kafkabackup.common.Constants;
import de.azapps.kafkabackup.common.partition.PartitionIndex;
import de.azapps.kafkabackup.common.partition.PartitionWriter;
import de.azapps.kafkabackup.common.segment.SegmentIndex;
import de.azapps.kafkabackup.common.record.Record;
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
	private Path targetDir;
	private Map<TopicPartition, PartitionWriter> partitionWriters = new HashMap<>();
	private long maxSegmentSize;

	@Override
	public String version() {
		return Constants.VERSION;
	}

	@Override
	public void start(Map<String, String> props) {
		try {
			if(!props.containsKey(Constants.TARGET_DIR_CONFIG)) {
				throw new RuntimeException("Missing Configuration Variable: " + Constants.TARGET_DIR_CONFIG);
			}
			if(!props.containsKey(Constants.MAX_SEGMENT_SIZE)) {
				throw new RuntimeException("Missing Configuration Variable: " + Constants.MAX_SEGMENT_SIZE);
			}
			targetDir = Paths.get(props.get(Constants.TARGET_DIR_CONFIG));
			maxSegmentSize = Long.parseLong(props.get(Constants.MAX_SEGMENT_SIZE));
			Files.createDirectories(targetDir);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private PartitionWriter preparePartition(TopicPartition topicPartition) throws IOException, PartitionIndex.IndexException {
		Path topicDir = Paths.get(targetDir.toString(), topicPartition.topic());
		Files.createDirectories(topicDir);
		return new PartitionWriter(topicPartition.topic(), topicPartition.partition(), topicDir, maxSegmentSize);
	}

	@Override
	public void open(Collection<TopicPartition> partitions) {
		try {
			for (TopicPartition topicPartition : partitions) {
				PartitionWriter partition = preparePartition(topicPartition);
				this.partitionWriters.put(topicPartition, partition);
			}
		} catch (IOException e) {
			throw new RuntimeException("IOException", e);
		} catch (PartitionIndex.IndexException e) {
			throw new RuntimeException("IndexException", e);
		}

	}

	@Override
	public void put(Collection<SinkRecord> records) {
		try {
			for (SinkRecord sinkRecord : records) {
				TopicPartition topicPartition = new TopicPartition(sinkRecord.topic(), sinkRecord.kafkaPartition());
				PartitionWriter partition = partitionWriters.get(topicPartition);
				partition.append(Record.fromSinkRecord(sinkRecord));
			}
		} catch (IOException | SegmentIndex.IndexException e ) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void stop() {
		try {
			for (PartitionWriter partition : partitionWriters.values()) {
				partition.close();
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
		try {
			for (PartitionWriter partition : partitionWriters.values()) {
				partition.flush();
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

}
