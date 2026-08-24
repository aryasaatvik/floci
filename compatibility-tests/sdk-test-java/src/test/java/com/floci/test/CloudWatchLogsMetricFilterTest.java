package com.floci.test;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.MetricFilter;
import software.amazon.awssdk.services.cloudwatchlogs.model.MetricTransformation;
import software.amazon.awssdk.services.cloudwatchlogs.model.StandardUnit;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("CloudWatch Logs metric filters")
class CloudWatchLogsMetricFilterTest {

    private static final String GROUP = "/test/" + TestFixtures.uniqueName("metric-filter");
    private static final String FILTER = "requests";

    private static CloudWatchLogsClient logs;

    @BeforeAll
    static void setUp() {
        logs = TestFixtures.cloudWatchLogsClient();
        logs.createLogGroup(b -> b.logGroupName(GROUP));
    }

    @AfterAll
    static void tearDown() {
        if (logs != null) {
            logs.deleteLogGroup(b -> b.logGroupName(GROUP));
            logs.close();
        }
    }

    @Test
    void putAndDescribeRoundTripsStableFullShape() {
        MetricTransformation transformation = MetricTransformation.builder()
                .metricName("Requests")
                .metricNamespace("Test")
                .metricValue("1")
                .defaultValue(0.0)
                .dimensions(Map.of("Service", "api", "Stage", "local"))
                .unit(StandardUnit.COUNT)
                .build();

        put(transformation);
        MetricFilter first = describe();
        put(transformation);
        MetricFilter second = describe();

        assertThat(first.filterPattern()).isEmpty();
        assertThat(first.applyOnTransformedLogs()).isTrue();
        assertThat(first.metricTransformations()).containsExactly(transformation);
        assertThat(second.metricTransformations()).isEqualTo(first.metricTransformations());
        assertThat(second.applyOnTransformedLogs()).isEqualTo(first.applyOnTransformedLogs());
        assertThat(second.creationTime()).isEqualTo(first.creationTime());
    }

    private static void put(MetricTransformation transformation) {
        logs.putMetricFilter(b -> b
                .logGroupName(GROUP)
                .filterName(FILTER)
                .filterPattern("")
                .metricTransformations(transformation)
                .applyOnTransformedLogs(true));
    }

    private static MetricFilter describe() {
        return logs.describeMetricFilters(b -> b.logGroupName(GROUP).filterNamePrefix(FILTER))
                .metricFilters().stream()
                .filter(filter -> FILTER.equals(filter.filterName()))
                .findFirst()
                .orElseThrow();
    }
}
