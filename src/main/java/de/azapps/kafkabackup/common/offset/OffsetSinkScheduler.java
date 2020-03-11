package de.azapps.kafkabackup.common.offset;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class OffsetSinkScheduler {
    private static final Logger log = LoggerFactory.getLogger(OffsetSinkScheduler.class);

    private OffsetSink offsetSink;
    private ScheduledFuture<?> handle;
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
        handle.cancel(false); // let it complete current run
        log.info("Stopped OffsetSinkScheduler");
    }
}
