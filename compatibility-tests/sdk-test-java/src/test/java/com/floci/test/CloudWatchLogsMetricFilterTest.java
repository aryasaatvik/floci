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
    private static final String OTHER_GROUP = GROUP + "-other";
    private static final String FILTER = TestFixtures.uniqueName("requests");

    private static CloudWatchLogsClient logs;

    @BeforeAll
    static void setUp() {
        logs = TestFixtures.cloudWatchLogsClient();
        logs.createLogGroup(request -> request.logGroupName(GROUP));
        logs.createLogGroup(request -> request.logGroupName(OTHER_GROUP));
    }

    @AfterAll
    static void tearDown() {
        if (logs != null) {
            deleteGroupIfPresent(GROUP);
            deleteGroupIfPresent(OTHER_GROUP);
            logs.close();
        }
    }

    @Test
    void sdkPutDescribeUpsertAndDeleteRoundTrip() {
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

        logs.putMetricFilter(request -> request
                .logGroupName(OTHER_GROUP)
                .filterName(FILTER)
                .filterPattern("")
                .metricTransformations(transformation));
        var accountWideFirst = logs.describeMetricFilters(request -> request
                .filterNamePrefix(FILTER)
                .limit(1));
        var accountWideSecond = logs.describeMetricFilters(request -> request
                .filterNamePrefix(FILTER)
                .limit(1)
                .nextToken(accountWideFirst.nextToken()));
        assertThat(accountWideFirst.metricFilters()).extracting(MetricFilter::logGroupName)
                .containsExactly(GROUP);
        assertThat(accountWideSecond.metricFilters()).extracting(MetricFilter::logGroupName)
                .containsExactly(OTHER_GROUP);
        assertThat(accountWideSecond.nextToken()).isNull();

        logs.deleteMetricFilter(request -> request.logGroupName(GROUP).filterName(FILTER));
        assertThat(logs.describeMetricFilters(request -> request.logGroupName(GROUP).filterNamePrefix(FILTER))
                .metricFilters()).isEmpty();
    }

    private static void deleteGroupIfPresent(String groupName) {
        boolean groupExists = logs.describeLogGroups(request -> request.logGroupNamePrefix(groupName))
                .logGroups().stream()
                .anyMatch(group -> groupName.equals(group.logGroupName()));
        if (groupExists) {
            logs.deleteLogGroup(request -> request.logGroupName(groupName));
        }
    }

    private static void put(MetricTransformation transformation) {
        logs.putMetricFilter(request -> request
                .logGroupName(GROUP)
                .filterName(FILTER)
                .filterPattern("")
                .metricTransformations(transformation)
                .applyOnTransformedLogs(true));
    }

    private static MetricFilter describe() {
        return logs.describeMetricFilters(request -> request.logGroupName(GROUP).filterNamePrefix(FILTER))
                .metricFilters().stream()
                .filter(filter -> FILTER.equals(filter.filterName()))
                .findFirst()
                .orElseThrow();
    }
}
