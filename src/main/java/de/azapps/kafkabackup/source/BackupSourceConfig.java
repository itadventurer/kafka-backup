package de.azapps.kafkabackup.source;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BackupSourceConfig extends AbstractConfig {
    private static final String CLUSTER_PREFIX = "cluster.";
    private static final String BATCH_SIZE_CONFIG = "batch.size";
    public static final String SOURCE_DIR_CONFIG = "source.dir";
    public static final String TOPICS_CONFIG = "topics";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(SOURCE_DIR_CONFIG, ConfigDef.Type.STRING,
                    ConfigDef.Importance.HIGH, "TargetDir")
            .define(BATCH_SIZE_CONFIG, ConfigDef.Type.INT, 100,
                    ConfigDef.Importance.LOW, "Batch size per partition")
            .define(TOPICS_CONFIG, ConfigDef.Type.STRING,
                    ConfigDef.Importance.HIGH, "Topics to restore");

    public BackupSourceConfig(Map<?, ?> props) {
        super(CONFIG_DEF, props);
        if (!props.containsKey(SOURCE_DIR_CONFIG)) {
            throw new RuntimeException("Missing Configuration Variable: " + SOURCE_DIR_CONFIG);
        }
        if (!props.containsKey(TOPICS_CONFIG)) {
            throw new RuntimeException("Missing Configuration Variable: " + TOPICS_CONFIG);
        }
    }

    Map<String, Object> consumerConfig() {
        Map<String, Object> props = new HashMap<>();
        props.putAll(originalsWithPrefix(CLUSTER_PREFIX));
        return props;
    }

    public String sourceDir() {
        return getString(SOURCE_DIR_CONFIG);
    }

    public Integer batchSize() {
        return getInt(BATCH_SIZE_CONFIG);
    }

    public List<String> topics() {
        return Arrays.asList(getString(TOPICS_CONFIG).split("\\s*,\\s*"));
    }


}

