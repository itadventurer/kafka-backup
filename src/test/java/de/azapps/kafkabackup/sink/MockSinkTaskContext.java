package de.azapps.kafkabackup.sink;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkTaskContext;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class MockSinkTaskContext implements SinkTaskContext {

    @Override
    public Map<String, String> configs() {
        return new HashMap<>();
    }

    @Override
    public void offset(Map<TopicPartition, Long> offsets) {

    }

    @Override
    public void offset(TopicPartition tp, long offset) {

    }

    @Override
    public void timeout(long timeoutMs) {

    }

    @Override
    public Set<TopicPartition> assignment() {
        return null;
    }

    @Override
    public void pause(TopicPartition... partitions) {

    }

    @Override
    public void resume(TopicPartition... partitions) {

    }

    @Override
    public void requestCommit() {

    }
}
