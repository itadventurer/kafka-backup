package de.azapps.kafkabackup.sink;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.HashMap;
import java.util.Map;

class BackupSinkConfig extends AbstractConfig {
    static final String CLUSTER_PREFIX = "cluster.";
    static final String CLUSTER_BOOTSTRAP_SERVERS = CLUSTER_PREFIX + "bootstrap.servers";
    static final String ADMIN_CLIENT_PREFIX = "admin.";
    static final String TARGET_DIR_CONFIG = "target.dir";
    static final String MAX_SEGMENT_SIZE = "max.segment.size.bytes";
    static final String SNAPSHOT = "snapshot";

    static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(TARGET_DIR_CONFIG, ConfigDef.Type.STRING,
                    ConfigDef.Importance.HIGH, "TargetDir")
            .define(MAX_SEGMENT_SIZE, ConfigDef.Type.INT, 1024 ^ 3, // 1 GiB
                    ConfigDef.Importance.LOW, "Maximum segment size")
            .define(SNAPSHOT, ConfigDef.Type.BOOLEAN, false,
                   ConfigDef.Importance.LOW, "Creates a snapshot. Terminates connector when end of all partitions has been reached.");

    BackupSinkConfig(Map<?, ?> props) {
        super(CONFIG_DEF, props, true);
        if (!props.containsKey(TARGET_DIR_CONFIG)) {
            throw new RuntimeException("Missing Configuration Variable: " + TARGET_DIR_CONFIG);
        }
        if (!props.containsKey(MAX_SEGMENT_SIZE)) {
            throw new RuntimeException("Missing Configuration Variable: " + MAX_SEGMENT_SIZE);
        }
    }

    Map<String, Object> adminConfig() {
        Map<String, Object> props = new HashMap<>();
        props.putAll(originalsWithPrefix(CLUSTER_PREFIX));
        props.putAll(originalsWithPrefix(ADMIN_CLIENT_PREFIX));
        return props;
    }

    String targetDir() {
        return getString(TARGET_DIR_CONFIG);
    }

    Integer maxSegmentSizeBytes() {
        return getInt(MAX_SEGMENT_SIZE);
    }

    Boolean snapShotMode() { return getBoolean(SNAPSHOT); }

    Map<String, Object> consumerConfig() {
        return new HashMap<>(originalsWithPrefix(CLUSTER_PREFIX));
    }

}
