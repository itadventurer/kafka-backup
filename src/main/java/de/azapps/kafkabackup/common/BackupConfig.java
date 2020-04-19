package de.azapps.kafkabackup.common;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public abstract class BackupConfig extends AbstractConfig {
    public static final String CLUSTER_PREFIX = "cluster.";
    public static final String CLUSTER_BOOTSTRAP_SERVERS = CLUSTER_PREFIX + "bootstrap.servers";
    public static final String KEY_CONVERTER = "key.converter";
    public static final String VALUE_CONVERTER = "value.converter";
    public static final String HEADER_CONVERTER = "header.converter";
    public static final String MANDATORY_CONVERTER = "org.apache.kafka.connect.converters.ByteArrayConverter";

    public BackupConfig(ConfigDef configDef, Map<?, ?> props) {
        super(configDef, props);
        if (!props.containsKey(CLUSTER_BOOTSTRAP_SERVERS)) {
            throw new RuntimeException("Missing Configuration Variable: " + CLUSTER_BOOTSTRAP_SERVERS);
        }

        if(!props.containsKey(KEY_CONVERTER) || ! props.get(KEY_CONVERTER).equals(MANDATORY_CONVERTER)) {
            throw new RuntimeException(KEY_CONVERTER + " must be set and must equal " + MANDATORY_CONVERTER);
        }

        if(!props.containsKey(VALUE_CONVERTER) || !props.get(VALUE_CONVERTER).equals(MANDATORY_CONVERTER)) {
            throw new RuntimeException(VALUE_CONVERTER + " must be set and must equal " + MANDATORY_CONVERTER);
        }

        if(!props.containsKey(HEADER_CONVERTER) || !props.get(HEADER_CONVERTER).equals(MANDATORY_CONVERTER)) {
            throw new RuntimeException(HEADER_CONVERTER + " must be set and must equal " + MANDATORY_CONVERTER);
        }
    }
}
