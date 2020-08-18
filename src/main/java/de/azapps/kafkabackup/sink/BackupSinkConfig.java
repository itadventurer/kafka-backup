package de.azapps.kafkabackup.sink;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

class BackupSinkConfig extends AbstractConfig {
    // Standard Kafka Connect Task configs
    static final String KEY_CONVERTER = "key.converter";
    static final String VALUE_CONVERTER = "value.converter";
    static final String HEADER_CONVERTER = "header.converter";
    static final String KAFKA_BYTE_ARRAY_CONVERTER_CLASS = "org.apache.kafka.connect.converters.ByteArrayConverter";

    // Custom kafka-backup sink task configs:
    static final String CLUSTER_PREFIX = "cluster.";
    static final String CLUSTER_BOOTSTRAP_SERVERS = CLUSTER_PREFIX + "bootstrap.servers";
    static final String ADMIN_CLIENT_PREFIX = "admin.";
    static final String TARGET_DIR_CONFIG = "target.dir";
    static final String MAX_SEGMENT_SIZE = "max.segment.size.bytes";
    static final String AWS_S3_REGION = "aws.s3.region";
    static final String AWS_S3_ENDPOINT = "aws.s3.endpoint";
    static final String AWS_S3_PATH_STYLE_ACCESS_ENABLED = "aws.s3.PathStyleAccessEnabled";
    static final String AWS_S3_BUCKET_NAME = "aws.s3.bucketName";
    static final String STORAGE_MODE = "storage.mode";
    static final String CONSUMER_GROUPS_SYNC_MAX_AGE_MS = "consumer.groups.sync.max.age.ms";
    static final String CONSUMER_OFFSET_SYNC_INTERVAL_MS = "consumer.offset.sync.interval.ms";
    static final String MAX_BATCH_MESSAGES = "max.batch.messages";
    static final String MAX_BATCH_TIME_MS = "max.batch.time.ms";

    static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(KEY_CONVERTER, ConfigDef.Type.STRING, KAFKA_BYTE_ARRAY_CONVERTER_CLASS,
                    ConfigDef.Importance.HIGH, "Standard Kafka Connect Task config, overriding the Worker default")
            .define(VALUE_CONVERTER, ConfigDef.Type.STRING, KAFKA_BYTE_ARRAY_CONVERTER_CLASS,
                    ConfigDef.Importance.HIGH, "Standard Kafka Connect Task config, overriding the Worker default")
            .define(HEADER_CONVERTER, ConfigDef.Type.STRING, KAFKA_BYTE_ARRAY_CONVERTER_CLASS,
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
                    ConfigDef.Importance.MEDIUM, "AWS S3 Bucket name")
            .define(CONSUMER_GROUPS_SYNC_MAX_AGE_MS, ConfigDef.Type.LONG, 300000,
                    ConfigDef.Importance.MEDIUM, "List of consumer groups for offset sync will be an most this old.")
            .define(CONSUMER_OFFSET_SYNC_INTERVAL_MS, ConfigDef.Type.LONG, 60000,
                    ConfigDef.Importance.MEDIUM, "Consumer offsets will be synced at this interval.")
            .define(MAX_BATCH_MESSAGES, ConfigDef.Type.INT, 1000,
                    ConfigDef.Importance.MEDIUM, "Batches will be rotated after this number of messages.")
            .define(MAX_BATCH_TIME_MS, ConfigDef.Type.LONG, 600000,
                    ConfigDef.Importance.MEDIUM, "Batches will be rotated after this amount of time.");

    BackupSinkConfig(Map<?, ?> props) {
        super(CONFIG_DEF, props);
        if (!(props.containsKey(KEY_CONVERTER) && getString(KEY_CONVERTER).equals(KAFKA_BYTE_ARRAY_CONVERTER_CLASS))) {
            // We NEED the key to be a byte array, so don't even start if task is using some other deserializer
            throw new RuntimeException(String.format("Configuration Variable `%s` needs to be set explicitly to `%s`", KEY_CONVERTER, KAFKA_BYTE_ARRAY_CONVERTER_CLASS));
        }
        if (!(props.containsKey(VALUE_CONVERTER) && getString(VALUE_CONVERTER).equals(KAFKA_BYTE_ARRAY_CONVERTER_CLASS))) {
            // We NEED the value to be a byte array, so don't even start if task is using some other deserializer
            throw new RuntimeException(String.format("Configuration Variable `%s` needs to be set explicitly to `%s`", VALUE_CONVERTER, KAFKA_BYTE_ARRAY_CONVERTER_CLASS));
        }
        if (!(props.containsKey(HEADER_CONVERTER) && getString(HEADER_CONVERTER).equals(KAFKA_BYTE_ARRAY_CONVERTER_CLASS))) {
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
        // First use envvars to populate certain props
        String saslMechanism = System.getenv("CONNECT_ADMIN_SASL_MECHANISM");
        if (saslMechanism != null) {
            props.put("sasl.mechanism", saslMechanism);
        }
        String securityProtocol = System.getenv("CONNECT_ADMIN_SECURITY_PROTOCOL");
        if (securityProtocol != null) {
            props.put("security.protocol", securityProtocol);
        }
        // NOTE: this is secret, so we *cannot* put it in the task config
        String saslJaasConfig = System.getenv("CONNECT_ADMIN_SASL_JAAS_CONFIG");
        if (saslJaasConfig != null) {
            props.put("sasl.jaas.config", saslJaasConfig);
        }
        // Then override with task config
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
    Long consumerGroupsSyncMaxAgeMs() {
        return getLong(CONSUMER_GROUPS_SYNC_MAX_AGE_MS);
    }
    Long consumerOffsetSyncIntervalMs() {
        return getLong(CONSUMER_OFFSET_SYNC_INTERVAL_MS);
    }
    Integer maxBatchMessages() { return getInt(MAX_BATCH_MESSAGES); }
    Long maxBatchTimeMs() { return getLong(MAX_BATCH_TIME_MS); }
}
