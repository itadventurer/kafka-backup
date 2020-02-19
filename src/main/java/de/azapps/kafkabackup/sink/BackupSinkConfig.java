package de.azapps.kafkabackup.sink;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

class BackupSinkConfig extends AbstractConfig {
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
