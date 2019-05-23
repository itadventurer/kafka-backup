package de.azapps.kafkabackup.sink;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.HashMap;
import java.util.Map;

public class BackupSinkConfig extends AbstractConfig {
    private static final String CLUSTER_PREFIX = "cluster.";
    private static final String ADMIN_CLIENT_PREFIX = "admin.";
    private static final String TARGET_DIR_CONFIG = "target.dir";
    private static final String MAX_SEGMENT_SIZE = "max.segment.size.bytes";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(TARGET_DIR_CONFIG, ConfigDef.Type.STRING,
                    ConfigDef.Importance.HIGH, "TargetDir")
            .define(MAX_SEGMENT_SIZE, ConfigDef.Type.INT, 1024 ^ 3, // 1 GiB
                    ConfigDef.Importance.LOW, "Maximum segment size");

    public BackupSinkConfig(Map<?, ?> props) {
        super(CONFIG_DEF, props);
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

    public String targetDir() {
        return getString(TARGET_DIR_CONFIG);
    }

    public Integer maxSegmentSize() {
        return getInt(MAX_SEGMENT_SIZE);
    }


}
