package de.azapps.kafkabackup.sink;

import de.azapps.kafkabackup.common.offset.OffsetSink;
import de.azapps.kafkabackup.common.partition.PartitionIndex;
import de.azapps.kafkabackup.common.partition.PartitionWriter;
import de.azapps.kafkabackup.common.record.Record;
import de.azapps.kafkabackup.common.segment.SegmentIndex;
import de.azapps.kafkabackup.common.segment.SegmentWriter;
import org.apache.kafka.clients.admin.AdminClient;
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
    private OffsetSink offsetSink;
	private BackupSinkConfig config;

	@Override
	public String version() {
		return "0.1";
	}

	@Override
	public void start(Map<String, String> props) {
		config = new BackupSinkConfig(props);
		try {
			targetDir = Paths.get(config.targetDir());
			maxSegmentSize = config.maxSegmentSize();
			Files.createDirectories(targetDir);

			// Setup OffsetSink
            AdminClient adminClient = AdminClient.create(config.adminConfig());
			offsetSink = new OffsetSink(adminClient, targetDir);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private PartitionWriter preparePartition(TopicPartition topicPartition) throws IOException, PartitionIndex.IndexException, SegmentIndex.IndexException {
		Path topicDir = Paths.get(targetDir.toString(), topicPartition.topic());
		Files.createDirectories(topicDir);
		return new PartitionWriter(topicPartition.topic(), topicPartition.partition(), topicDir, maxSegmentSize);
	}

    private void addPartitionWriter(TopicPartition topicPartition) throws PartitionIndex.IndexException, IOException, SegmentIndex.IndexException {
		PartitionWriter partition = preparePartition(topicPartition);
		this.partitionWriters.put(topicPartition, partition);
	}

	@Override
	public void put(Collection<SinkRecord> records) {
		try {
			for (SinkRecord sinkRecord : records) {
				TopicPartition topicPartition = new TopicPartition(sinkRecord.topic(), sinkRecord.kafkaPartition());
				if(!partitionWriters.containsKey(topicPartition)) {
					addPartitionWriter(topicPartition);
				}
				PartitionWriter partition = partitionWriters.get(topicPartition);
				partition.append(Record.fromSinkRecord(sinkRecord));
			}
			// Todo: refactor to own worker. E.g. using the scheduler of MM2
			offsetSink.syncConsumerGroups();
			offsetSink.syncOffsets();
		} catch (IOException | SegmentIndex.IndexException | PartitionIndex.IndexException | SegmentWriter.SegmentException e ) {
			throw new RuntimeException(e);
        }
    }

	@Override
	public void stop() {
		try {
			for (PartitionWriter partition : partitionWriters.values()) {
				partition.close();
			}
			offsetSink.close();
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
			offsetSink.flush();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

}
