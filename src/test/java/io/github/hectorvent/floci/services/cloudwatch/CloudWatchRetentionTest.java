package io.github.hectorvent.floci.services.cloudwatch;

import com.fasterxml.jackson.core.type.TypeReference;
import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.core.common.RegionResolver;
import io.github.hectorvent.floci.core.common.ServiceConfigAccess;
import io.github.hectorvent.floci.core.storage.StorageFactory;
import io.github.hectorvent.floci.services.cloudwatch.logs.CloudWatchLogsService;
import io.github.hectorvent.floci.services.cloudwatch.logs.model.LogEvent;
import io.github.hectorvent.floci.services.cloudwatch.metrics.CloudWatchMetricsService;
import io.github.hectorvent.floci.services.cloudwatch.metrics.model.MetricDatum;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class CloudWatchRetentionTest {
    @TempDir Path directory;
    private static final String ACCOUNT = "111111111111";
    private static final String OTHER = "222222222222";
    private static final String REGION = "us-east-1";

    @Test
    void sweepBoundsLoadedMetricAndLogHistoryAcrossAccountsAndKeepsBoundary() {
        EmulatorConfig config = config(600, 86400);
        ServiceConfigAccess access = mock(ServiceConfigAccess.class);
        when(access.storageMode(anyString())).thenReturn("persistent");
        when(access.storageFlushInterval(anyString())).thenReturn(60_000L);
        StorageFactory factory = new StorageFactory(config, access);
        RegionResolver region = new RegionResolver(REGION, ACCOUNT);
        CloudWatchMetricsService metrics = new CloudWatchMetricsService(factory, region);
        CloudWatchLogsService logs = new CloudWatchLogsService(factory, config, region, null, null);
        Instant now = Instant.now();
        for (String account : List.of(ACCOUNT, OTHER)) {
            metrics.putMetricDataForAccount(account, "custom", List.of(
                    MetricDatum.of("value", "Count", 1, List.of(), now.minusSeconds(601).getEpochSecond()),
                    MetricDatum.of("value", "Count", 2, List.of(), now.minusSeconds(600).getEpochSecond())), REGION);
            logs.createLogGroupForAccount(account, "/ttl", null, null, REGION);
            logs.createLogStreamForAccount(account, "/ttl", "s", REGION);
            logs.putLogEventsForAccount(account, "/ttl", "s", List.of(
                    Map.of("message", "old", "timestamp", now.minusSeconds(86401).toEpochMilli()),
                    Map.of("message", "boundary", "timestamp", now.minusSeconds(86400).toEpochMilli())), REGION);
        }
        CloudWatchRetention retention = new CloudWatchRetention(metrics, logs, config);
        try {
            retention.sweep(now);
            assertEquals(2, factory.create("cloudwatchmetrics", "cwmetrics.json",
                    new TypeReference<Map<String, MetricDatum>>() {}).scanAllAccounts().size());
            assertEquals(List.of("boundary", "boundary"), factory.create("cloudwatchlogs", "cwlogs-events.json",
                    new TypeReference<Map<String, LogEvent>>() {}).scanAllAccounts().stream().map(LogEvent::getMessage).toList());
        } finally {
            retention.stop(null);
            factory.shutdownAll();
        }
    }

    @Test
    void disabledCeilingsDoNotChangeExistingDefaults() {
        CloudWatchMetricsService metrics = mock(CloudWatchMetricsService.class);
        CloudWatchLogsService logs = mock(CloudWatchLogsService.class);
        CloudWatchRetention retention = new CloudWatchRetention(metrics, logs, config(0, 0));
        retention.sweep(Instant.now());
        retention.stop(null);
        verifyNoInteractions(metrics, logs);
    }

    private EmulatorConfig config(long metricSeconds, long logSeconds) {
        EmulatorConfig config = mock(EmulatorConfig.class, RETURNS_DEEP_STUBS);
        when(config.defaultAccountId()).thenReturn(ACCOUNT);
        when(config.storage().persistentPath()).thenReturn(directory.toString());
        when(config.storage().wal().compactionIntervalMs()).thenReturn(60_000L);
        when(config.services().cloudwatchlogs().maxEventsPerQuery()).thenReturn(10_000);
        when(config.services().cloudwatchlogs().maxStoredEvents()).thenReturn(20_000);
        when(config.services().cloudwatchlogs().retentionSeconds()).thenReturn(logSeconds);
        when(config.services().cloudwatchmetrics().retentionSeconds()).thenReturn(metricSeconds);
        return config;
    }
}
