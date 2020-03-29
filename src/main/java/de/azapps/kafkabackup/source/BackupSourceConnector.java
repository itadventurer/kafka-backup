package de.azapps.kafkabackup.source;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class BackupSourceConnector extends SourceConnector {
    private Map<String, String> config;


    @Override
    public void start(Map<String, String> props) {
        config = props;
        if (!config.getOrDefault(BackupSourceConfig.ALLOW_OLD_KAFKA_CONNECT_VERSION, "false").equals("true")) {
            try {
                SourceTask.class.getMethod("commitRecord", SourceRecord.class, RecordMetadata.class);
            } catch (NoSuchMethodException e) {
                throw new RuntimeException("Kafka Backup requires at least Kafka Connect 2.4. Otherwise Offsets cannot be committed. If you are sure what you are doing, please set " + BackupSourceConfig.ALLOW_OLD_KAFKA_CONNECT_VERSION + " to true");
            }
        }
    }

    @Override
    public Class<? extends Task> taskClass() {
        return BackupSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        if (maxTasks > 1) {
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
        return new ConfigDef();
    }

    @Override
    public String version() {
        return "0.1";
    }
}
