package de.azapps.kafkabackup.sink;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class BackupSinkConnector extends SinkConnector {
	private Map<String, String> config;

	@Override
	public void start(Map<String, String> props) {
		config = props;
	}

	@Override
	public Class<? extends Task> taskClass() {
		return BackupSinkTask.class;
	}

	@Override
	public List<Map<String, String>> taskConfigs(int maxTasks) {
		if (maxTasks > 1 && config.get(BackupSinkConfig.STORAGE_MODE).equals(StorageMode.DISK.name())) {
			throw new ConnectException("kafka-backup can currently handle only one task using storage mode: "
					+ StorageMode.DISK.name());
		}
		// We need to return a list of configurations, where the size should be equal to maxTasks,
		// otherwise the connector will not be scaled.
		// See https://www.confluent.jp/blog/create-dynamic-kafka-connect-source-connectors/
		// for a great guide to implementing the taskConfigs method.
		List<Map<String, String>> configs = new ArrayList<>(maxTasks);
		for (int i = 0; i < maxTasks; i++) {
			configs.add(config); // Just give the same config to all tasks
		}
		return configs;
	}

	@Override
	public void stop() {

	}

	@Override
	public ConfigDef config() {
		return BackupSinkConfig.CONFIG_DEF;
	}

	@Override
	public String version() {
		return "0.1";
	}
}
