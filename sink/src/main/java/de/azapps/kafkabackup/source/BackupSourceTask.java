package de.azapps.kafkabackup.source;

import de.azapps.kafkabackup.common.Constants;
import de.azapps.kafkabackup.common.segment.Segment;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BackupSourceTask extends SourceTask {
    private Path targetDir;
    private Map<TopicPartition, Segment> partitionSegments = new HashMap<>();
    private int batchSize;

    @Override
    public String version() {
        return Constants.VERSION;
    }

    @Override
    public void start(Map<String, String> props) {
        targetDir = Paths.get(props.get(Constants.TARGET_DIR_CONFIG));
        batchSize = Integer.parseInt(props.get(Constants.BATCH_SIZE_CONFIG));
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        return null;
    }

    @Override
    public void stop() {

    }
}
