package io.github.hectorvent.floci.services.cloudwatch.metrics;

import io.github.hectorvent.floci.services.cloudwatch.dashboards.CloudWatchDashboardsService;
import io.github.hectorvent.floci.services.cloudwatch.metricstreams.CloudWatchMetricStreamsService;
import io.github.hectorvent.floci.services.cloudwatch.metrics.model.CompositeAlarm;
import io.github.hectorvent.floci.services.cloudwatch.metrics.model.MetricAlarm;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.Response;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The JSON 1.0 handler defaults {@code ActionsEnabled} to {@code true} when omitted, matching
 * AWS's documented default for {@code PutMetricAlarm}. This handler must do the same over the
 * Query protocol — a mismatch here would silently disable {@link AlarmEvaluator} dispatch for
 * any hand-created alarm (e.g. behind a StepScaling policy) that doesn't explicitly pass the
 * parameter, since {@code Boolean.parseBoolean(null)} defaults to {@code false}.
 */
class CloudWatchMetricsQueryHandlerTest {

    private static final String REGION = "us-east-1";

    private final CloudWatchMetricsService metricsService = mock(CloudWatchMetricsService.class);
    // The alarm cases below never reach a dashboard operation; the handler simply routes both.
    private final CloudWatchMetricsQueryHandler handler = new CloudWatchMetricsQueryHandler(
            metricsService, mock(CloudWatchDashboardsService.class),
            mock(CloudWatchMetricStreamsService.class));

    @Test
    void putMetricAlarmDefaultsActionsEnabledToTrueWhenOmitted() {
        MultivaluedMap<String, String> params = new MultivaluedHashMap<>();
        params.putSingle("AlarmName", "TestAlarm");
        params.putSingle("MetricName", "M");
        params.putSingle("Namespace", "NS");

        handler.handle("PutMetricAlarm", params, REGION);

        ArgumentCaptor<MetricAlarm> captor = ArgumentCaptor.forClass(MetricAlarm.class);
        verify(metricsService).putMetricAlarm(captor.capture(), anyString());
        assertTrue(captor.getValue().isActionsEnabled());
    }

    @Test
    void putMetricAlarmRespectsExplicitActionsEnabledFalse() {
        MultivaluedMap<String, String> params = new MultivaluedHashMap<>();
        params.putSingle("AlarmName", "TestAlarm");
        params.putSingle("MetricName", "M");
        params.putSingle("Namespace", "NS");
        params.putSingle("ActionsEnabled", "false");

        handler.handle("PutMetricAlarm", params, REGION);

        ArgumentCaptor<MetricAlarm> captor = ArgumentCaptor.forClass(MetricAlarm.class);
        verify(metricsService).putMetricAlarm(captor.capture(), anyString());
        assertFalse(captor.getValue().isActionsEnabled());
    }

    @Test
    void putCompositeAlarmParsesRuleActionsAndTags() {
        MultivaluedMap<String, String> params = new MultivaluedHashMap<>();
        params.putSingle("AlarmName", "CompositeOne");
        params.putSingle("AlarmRule", "ALARM(\"MetricOne\")");
        params.putSingle("AlarmDescription", "rollup");
        params.putSingle("AlarmActions.member.1", "arn:aws:sns:us-east-1:000000000000:topic");
        params.putSingle("Tags.member.1.Key", "env");
        params.putSingle("Tags.member.1.Value", "test");

        handler.handle("PutCompositeAlarm", params, REGION);

        ArgumentCaptor<CompositeAlarm> captor = ArgumentCaptor.forClass(CompositeAlarm.class);
        verify(metricsService).putCompositeAlarm(captor.capture(), anyString());
        CompositeAlarm alarm = captor.getValue();
        assertEquals("CompositeOne", alarm.getAlarmName());
        assertEquals("ALARM(\"MetricOne\")", alarm.getAlarmRule());
        assertEquals("rollup", alarm.getAlarmDescription());
        assertEquals("arn:aws:sns:us-east-1:000000000000:topic", alarm.getAlarmActions().get(0));
        assertEquals("test", alarm.getTags().get("env"));
        assertTrue(alarm.isActionsEnabled());
    }

    @Test
    void describeAlarmsWithCompositeType_returnsCompositeArrayAndSkipsMetricLookup() {
        CompositeAlarm composite = new CompositeAlarm();
        composite.setAlarmName("CompositeOne");
        composite.setAlarmRule("ALARM(\"MetricOne\")");
        when(metricsService.describeCompositeAlarms(any(), any(), eq(REGION)))
                .thenReturn(List.of(composite));

        MultivaluedMap<String, String> params = new MultivaluedHashMap<>();
        params.putSingle("AlarmTypes.member.1", "CompositeAlarm");
        Response response = handler.handle("DescribeAlarms", params, REGION);

        assertEquals(200, response.getStatus());
        assertTrue(response.getEntity().toString().contains("CompositeAlarms"));
        assertFalse(response.getEntity().toString().contains("<MetricAlarms>"));
        verify(metricsService, never()).describeAlarms(any(), any(), anyString());
    }
}
