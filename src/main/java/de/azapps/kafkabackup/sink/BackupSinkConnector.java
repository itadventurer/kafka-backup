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
		if(maxTasks > 1 && config.get(BackupSinkConfig.STORAGE_MODE).equals(StorageMode.DISK.name())) {
			throw new ConnectException("kafka-backup can currently handle only one task using storage mode: "
					+ StorageMode.DISK.name());
		}
		List<Map<String, String>> configs = new ArrayList<>();
		configs.add(config);
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
