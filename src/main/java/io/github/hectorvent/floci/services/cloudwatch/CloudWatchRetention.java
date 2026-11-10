package io.github.hectorvent.floci.services.cloudwatch;

import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.services.cloudwatch.logs.CloudWatchLogsService;
import io.github.hectorvent.floci.services.cloudwatch.metrics.CloudWatchMetricsService;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/** Optional local-development age ceilings, independent of AWS group policies and alarm history. */
@ApplicationScoped
public class CloudWatchRetention {
    private static final Logger LOG = Logger.getLogger(CloudWatchRetention.class);
    private final CloudWatchMetricsService metrics;
    private final CloudWatchLogsService logs;
    private final long metricSeconds;
    private final long logSeconds;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread thread = new Thread(r, "cloudwatch-retention");
        thread.setDaemon(true);
        return thread;
    });

    @Inject
    public CloudWatchRetention(CloudWatchMetricsService metrics, CloudWatchLogsService logs, EmulatorConfig config) {
        this.metrics = metrics;
        this.logs = logs;
        this.metricSeconds = config.services().cloudwatchmetrics().retentionSeconds();
        this.logSeconds = config.services().cloudwatchlogs().retentionSeconds();
        if (metricSeconds < 0 || logSeconds < 0) {
            throw new IllegalArgumentException("CloudWatch retention-seconds must be nonnegative");
        }
    }

    void start(@Observes StartupEvent ignored) {
        if (metricSeconds == 0 && logSeconds == 0) {
            return;
        }
        sweep(Instant.now());
        // Applying a smaller ceiling to an existing volume must reclaim its old snapshot immediately.
        if (metricSeconds > 0) {
            metrics.flushMetricData();
        }
        if (logSeconds > 0) {
            logs.flushEvents();
        }
        scheduler.scheduleAtFixedRate(() -> {
            try {
                sweep(Instant.now());
            } catch (RuntimeException e) {
                LOG.errorv(e, "CloudWatch retention sweep failed; the next minute retries");
            }
        }, 60, 60, TimeUnit.SECONDS);
    }

    void sweep(Instant now) {
        if (metricSeconds > 0) {
            metrics.deleteMetricDataBefore(now.minusSeconds(metricSeconds));
        }
        if (logSeconds > 0) {
            logs.deleteEventsBefore(now.minusSeconds(logSeconds).toEpochMilli());
        }
    }

    void stop(@Observes @Priority(1000) ShutdownEvent ignored) {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
