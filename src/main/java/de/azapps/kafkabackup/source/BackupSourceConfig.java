package de.azapps.kafkabackup.source;

import de.azapps.kafkabackup.common.BackupConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class BackupSourceConfig extends BackupConfig {
    private static final String CLUSTER_KEY_DESERIALIZER = CLUSTER_PREFIX + "key.deserializer";
    private static final String CLUSTER_VALUE_DESERIALIZER = CLUSTER_PREFIX + "value.deserializer";
    private static final String BATCH_SIZE_CONFIG = "batch.size";
    private static final String SOURCE_DIR_CONFIG = "source.dir";
    private static final String TOPICS_CONFIG = "topics";
    static final String ALLOW_OLD_KAFKA_CONNECT_VERSION = "allow.old.kafka.connect.version.unsafe";


    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(SOURCE_DIR_CONFIG, ConfigDef.Type.STRING,
                    ConfigDef.Importance.HIGH, "TargetDir")
            .define(BATCH_SIZE_CONFIG, ConfigDef.Type.INT, 100,
                    ConfigDef.Importance.LOW, "Batch size per partition")
            .define(TOPICS_CONFIG, ConfigDef.Type.STRING,
                    ConfigDef.Importance.HIGH, "Topics to restore");

    BackupSourceConfig(Map<?, ?> props) {
        super(CONFIG_DEF, props);
        if (!props.containsKey(SOURCE_DIR_CONFIG)) {
            throw new RuntimeException("Missing Configuration Variable: " + SOURCE_DIR_CONFIG);
        }
        if (!props.containsKey(TOPICS_CONFIG)) {
            throw new RuntimeException("Missing Configuration Variable: " + TOPICS_CONFIG);
        }
        if (!props.containsKey(CLUSTER_KEY_DESERIALIZER)) {
            throw new RuntimeException("Missing Configuration Variable: " + CLUSTER_KEY_DESERIALIZER);
        }
        if (!props.containsKey(CLUSTER_VALUE_DESERIALIZER)) {
            throw new RuntimeException("Missing Configuration Variable: " + CLUSTER_VALUE_DESERIALIZER);
        }
    }

    Map<String, Object> consumerConfig() {
        return new HashMap<>(originalsWithPrefix(CLUSTER_PREFIX));
    }

    String sourceDir() {
        return getString(SOURCE_DIR_CONFIG);
    }

    Integer batchSize() {
        return getInt(BATCH_SIZE_CONFIG);
    }

    List<String> topics() {
        return Arrays.asList(getString(TOPICS_CONFIG).split("\\s*,\\s*"));
    }


}

