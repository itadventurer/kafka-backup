package de.azapps.kafkabackup.common.offset;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@Slf4j
public class OffsetSinkScheduler {

    private ScheduledFuture<?> handle;
    private final OffsetSink offsetSink;
    private final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);

    public OffsetSinkScheduler(OffsetSink offsetSink) {
        this.offsetSink = offsetSink;
    }

    public void start(long syncIntervalMs) {
        log.info("Started OffsetSinkScheduler");
        final Runnable task = () -> {
            log.info("Syncing offsets...");
            offsetSink.syncOffsets();
            offsetSink.flush();
            log.info("Syncing offsets... done");
        };
        handle = executorService.scheduleAtFixedRate(task, 0, syncIntervalMs, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        if (handle != null) {
            handle.cancel(false); // let it complete current run
        }
        log.info("Stopped OffsetSinkScheduler");
    }
}
