package de.azapps.kafkabackup.sink;

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
        List<Map<String, String>> configs = new ArrayList<>();
        for(int i = 0; i < maxTasks; i++) {
            Map<String, String> newConfig = new HashMap<>(config);
            newConfig.put("task.id", String.valueOf(i));
            configs.add(newConfig);
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
