package de.azapps.kafkabackup.sink;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

class BackupSinkConfig extends AbstractConfig {
    // Standard Kafka Connect Task configs
    private static final String KEY_CONVERTER = "key.converter";
    private static final String VALUE_CONVERTER = "value.converter";
    private static final String HEADER_CONVERTER = "header.converter";
    private static final String KAFKA_BYTE_ARRAY_CONVERTER_CLASS = "org.apache.kafka.connect.converters.ByteArrayConverter";

    // Custom kafka-backup sink task configs:
    private static final String CLUSTER_PREFIX = "cluster.";
    private static final String CLUSTER_BOOTSTRAP_SERVERS = CLUSTER_PREFIX + "bootstrap.servers";
    private static final String ADMIN_CLIENT_PREFIX = "admin.";
    private static final String TARGET_DIR_CONFIG = "target.dir";
    private static final String MAX_SEGMENT_SIZE = "max.segment.size.bytes";
    private static final String AWS_S3_REGION = "aws.s3.region";
    private static final String AWS_S3_ENDPOINT = "aws.s3.endpoint";
    private static final String AWS_S3_PATH_STYLE_ACCESS_ENABLED = "aws.s3.PathStyleAccessEnabled";
    private static final String AWS_S3_BUCKET_NAME = "aws.s3.bucketName";
    private static final String STORAGE_MODE = "storage.mode";

    static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(KEY_CONVERTER, ConfigDef.Type.STRING,
                    ConfigDef.Importance.HIGH, "Standard Kafka Connect Task config, overriding the Worker default")
            .define(VALUE_CONVERTER, ConfigDef.Type.STRING,
                    ConfigDef.Importance.HIGH, "Standard Kafka Connect Task config, overriding the Worker default")
            .define(HEADER_CONVERTER, ConfigDef.Type.STRING,
                    ConfigDef.Importance.HIGH, "Standard Kafka Connect Task config, overriding the Worker default")
            .define(STORAGE_MODE, ConfigDef.Type.STRING,
                    ConfigDef.Importance.HIGH, "Where to store the backups. DISK or S3")
            .define(TARGET_DIR_CONFIG, ConfigDef.Type.STRING,
                    ConfigDef.Importance.HIGH, "TargetDir")
            .define(MAX_SEGMENT_SIZE, ConfigDef.Type.INT, 1024 ^ 3, // 1 GiB
                    ConfigDef.Importance.LOW, "Maximum segment size")
            .define(AWS_S3_REGION, ConfigDef.Type.STRING,
                    ConfigDef.Importance.MEDIUM, "AWS S3 Bucket region")
            .define(AWS_S3_ENDPOINT, ConfigDef.Type.STRING,
                    ConfigDef.Importance.MEDIUM, "AWS S3 Bucket endpoint")
            .define(AWS_S3_PATH_STYLE_ACCESS_ENABLED, ConfigDef.Type.BOOLEAN,
                    ConfigDef.Importance.MEDIUM, "AWS S3 Bucket path style access")
            .define(AWS_S3_BUCKET_NAME, ConfigDef.Type.STRING,
                    ConfigDef.Importance.MEDIUM, "AWS S3 Bucket name");

    BackupSinkConfig(Map<?, ?> props) {
        super(CONFIG_DEF, props);
        if (!(props.containsKey(KEY_CONVERTER) && getString(KEY_CONVERTER) == KAFKA_BYTE_ARRAY_CONVERTER_CLASS)) {
            // We NEED the key to be a byte array, so don't even start if task is using some other deserializer
            throw new RuntimeException(String.format("Configuration Variable `%s` needs to be set explicitly to `%s`", KEY_CONVERTER, KAFKA_BYTE_ARRAY_CONVERTER_CLASS));
        }
        if (!(props.containsKey(VALUE_CONVERTER) && getString(VALUE_CONVERTER) == KAFKA_BYTE_ARRAY_CONVERTER_CLASS)) {
            // We NEED the value to be a byte array, so don't even start if task is using some other deserializer
            throw new RuntimeException(String.format("Configuration Variable `%s` needs to be set explicitly to `%s`", VALUE_CONVERTER, KAFKA_BYTE_ARRAY_CONVERTER_CLASS));
        }
        if (!(props.containsKey(HEADER_CONVERTER) && getString(HEADER_CONVERTER) == KAFKA_BYTE_ARRAY_CONVERTER_CLASS)) {
            // We NEED the header to be a byte array, so don't even start if task is using some other deserializer
            throw new RuntimeException(String.format("Configuration Variable `%s` needs to be set explicitly to `%s`", HEADER_CONVERTER, KAFKA_BYTE_ARRAY_CONVERTER_CLASS));
        }
        if (!props.containsKey(TARGET_DIR_CONFIG)) {
            throw new RuntimeException("Missing Configuration Variable: " + TARGET_DIR_CONFIG);
        }
        if (!props.containsKey(MAX_SEGMENT_SIZE)) {
            throw new RuntimeException("Missing Configuration Variable: " + MAX_SEGMENT_SIZE);
        }
        if(!props.containsKey(CLUSTER_BOOTSTRAP_SERVERS)) {
            throw new RuntimeException("Missing Configuration Variable: " + CLUSTER_BOOTSTRAP_SERVERS);
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

    Integer maxSegmentSize() {
        return getInt(MAX_SEGMENT_SIZE);
    }

    StorageMode storageMode() {
        return StorageMode.valueOf(getString(STORAGE_MODE));
    }

    String bucketName() {
        return getString(AWS_S3_BUCKET_NAME);
    }

    String endpoint() {
        return getString(AWS_S3_ENDPOINT);
    }
    Boolean pathStyleAccessEnabled() {
        return getBoolean(AWS_S3_PATH_STYLE_ACCESS_ENABLED);
    }
    String region() {
        return getString(AWS_S3_REGION);
    }
}
