package de.azapps.kafkabackup;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.HashMap;
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
		if(maxTasks>1) {
			throw new ConnectException("kafka-backup can currently handle only one task.");
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
		return null;
	}

	@Override
	public String version() {
		return Constants.VERSION;
	}
}
