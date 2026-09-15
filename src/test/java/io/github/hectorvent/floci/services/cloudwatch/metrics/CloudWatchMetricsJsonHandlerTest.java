package io.github.hectorvent.floci.services.cloudwatch.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.hectorvent.floci.core.common.RegionResolver;
import io.github.hectorvent.floci.core.storage.InMemoryStorage;
import io.github.hectorvent.floci.services.cloudwatch.dashboards.CloudWatchDashboardsService;
import io.github.hectorvent.floci.services.cloudwatch.metricstreams.CloudWatchMetricStreamsService;
import jakarta.ws.rs.core.Response;
import java.math.BigDecimal;
import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Handler-level tests for CloudWatchMetricsJsonHandler.
 *
 * The AWS SDK v2 serialises Instant values via DateUtils.formatUnixTimestampInstant(),
 * which produces a plain decimal epoch-second number (e.g. 1750000000.123) written
 * via JsonGenerator.writeNumber(String). Jackson deserialises this as a numeric node,
 * which is what these tests replicate using BigDecimal to avoid double-precision artefacts.
 */
class CloudWatchMetricsJsonHandlerTest {

    private static final String REGION = "us-east-1";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    // Fixed reference point — avoids wall-clock non-determinism.
    private static final Instant EPOCH_NOW = Instant.parse("2025-06-16T12:00:00Z");
    private static final Instant EPOCH_OLD = EPOCH_NOW.minusSeconds(86400);

    private CloudWatchMetricsJsonHandler handler;

    @BeforeEach
    void setUp() {
        CloudWatchMetricsService service = new CloudWatchMetricsService(
                new InMemoryStorage<>(),
                new InMemoryStorage<>(),
                new RegionResolver(REGION, "000000000000")
        );
        CloudWatchDashboardsService dashboardsService = new CloudWatchDashboardsService(
                new InMemoryStorage<>(),
                new RegionResolver(REGION, "000000000000")
        );
        CloudWatchMetricStreamsService metricStreamsService = new CloudWatchMetricStreamsService(
                new InMemoryStorage<>(), new RegionResolver(REGION, "000000000000"));
        handler = new CloudWatchMetricsJsonHandler(service, dashboardsService, metricStreamsService, MAPPER);
    }

    /**
     * Mimics DateUtils.formatUnixTimestampInstant: epoch millis as a decimal BigDecimal
     * (epoch seconds with millisecond precision). Using BigDecimal avoids the scientific-
     * notation and precision issues that arise when casting through double.
     */
    private static BigDecimal sdkTimestamp(Instant instant) {
        return new BigDecimal(instant.toEpochMilli()).scaleByPowerOfTen(-3);
    }

    private Response putMetric(String namespace, String metricName,
                                String dimName, String dimValue,
                                double value, Instant timestamp) {
        ObjectNode req = MAPPER.createObjectNode();
        req.put("Namespace", namespace);
        var datum = req.putArray("MetricData").addObject();
        datum.put("MetricName", metricName);
        datum.put("Value", value);
        datum.put("Timestamp", sdkTimestamp(timestamp));
        datum.putArray("Dimensions").addObject()
                .put("Name", dimName).put("Value", dimValue);
        return handler.handle("PutMetricData", req, REGION);
    }

    private ObjectNode getStats(String namespace, String metricName,
                                 String dimName, String dimValue,
                                 Instant startTime, Instant endTime, int period) {
        ObjectNode req = MAPPER.createObjectNode();
        req.put("Namespace", namespace);
        req.put("MetricName", metricName);
        req.put("Period", period);
        req.put("StartTime", sdkTimestamp(startTime));
        req.put("EndTime", sdkTimestamp(endTime));
        req.putArray("Dimensions").addObject()
                .put("Name", dimName).put("Value", dimValue);
        req.putArray("Statistics").add("Sum");
        Response resp = handler.handle("GetMetricStatistics", req, REGION);
        assertEquals(200, resp.getStatus());
        return (ObjectNode) resp.getEntity();
    }

    @Test
    void putMetricData_decimalEpochTimestamp_storesCorrectTimestamp() {
        assertEquals(200, putMetric("NS", "M", "type", "old", 200.0, EPOCH_OLD).getStatus());

        // Wide window around the old timestamp — must find the datapoint
        ObjectNode wide = getStats("NS", "M", "type", "old",
                EPOCH_OLD.minusSeconds(60), EPOCH_OLD.plusSeconds(60), 3600);
        assertEquals(1, wide.get("Datapoints").size(),
                "metric stored with 24h-ago timestamp must be found when querying around that time");

        // Narrow window around now — must not find the datapoint
        ObjectNode narrow = getStats("NS", "M", "type", "old",
                EPOCH_NOW.minusSeconds(10), EPOCH_NOW.plusSeconds(10), 60);
        assertEquals(0, narrow.get("Datapoints").size(),
                "metric stored with 24h-ago timestamp must not appear in a 20-second window around now");
    }

    @Test
    void setAlarmState_updatesFieldsCorrectly() {
        putMetric("NS", "M", "type", "current", 100.0, EPOCH_NOW);

        ObjectNode putAlarmReq = MAPPER.createObjectNode();
        putAlarmReq.put("AlarmName", "TestAlarm");
        putAlarmReq.put("MetricName", "M");
        putAlarmReq.put("Namespace", "NS");
        putAlarmReq.putArray("AlarmActions").add("alarm-action");
        putAlarmReq.putArray("OKActions").add("ok-action");
        putAlarmReq.putArray("InsufficientDataActions").add("insufficient-action");
        ArrayNode dimensions = putAlarmReq.putArray("Dimensions");
        dimensions.addObject().put("Name", "period").put("Value", "60");
        dimensions.addObject().put("Name", "count").put("Value", "2");
        Response putAlarmResp = handler.handle("PutMetricAlarm", putAlarmReq, REGION);
        assertEquals(200, putAlarmResp.getStatus());

        ObjectNode alarmReq = MAPPER.createObjectNode();
        alarmReq.put("AlarmName", "TestAlarm");
        alarmReq.put("StateValue", "ALARM");
        alarmReq.put("StateReason", "Test reason");
        alarmReq.put("StateReasonData", "{\"k\":\"v\"}");
        Response resp = handler.handle("SetAlarmState", alarmReq, REGION);
        assertEquals(200, resp.getStatus());

        // Verify that the alarm state is reflected in the metrics service
        ObjectNode getAlarmReq = MAPPER.createObjectNode();
        getAlarmReq.putArray("AlarmNames").add("TestAlarm");
        Response getResp = handler.handle("DescribeAlarms", getAlarmReq, REGION);
        assertEquals(200, getResp.getStatus());
        ObjectNode alarmData = (ObjectNode) ((ObjectNode) getResp.getEntity()).get("MetricAlarms").get(0);
        assertEquals("ALARM", alarmData.get("StateValue").asText());
        assertEquals("Test reason", alarmData.get("StateReason").asText());
        assertEquals("{\"k\":\"v\"}", alarmData.get("StateReasonData").asText());
        assertTrue(alarmData.path("AlarmActions").isArray());
        assertEquals("alarm-action", alarmData.path("AlarmActions").get(0).asText());
        assertTrue(alarmData.path("OKActions").isArray());
        assertEquals("ok-action", alarmData.path("OKActions").get(0).asText());
        assertTrue(alarmData.path("InsufficientDataActions").isArray());
        assertEquals("insufficient-action", alarmData.path("InsufficientDataActions").get(0).asText());
        assertTrue(alarmData.path("StateUpdatedTimestamp").asLong() > 0);
        assertTrue(alarmData.path("Dimensions").isArray());
        JsonNode dimensionPeriod = alarmData.path("Dimensions").get(0);
        assertEquals("period", dimensionPeriod.get("Name").asText());
        assertEquals("60", dimensionPeriod.get("Value").asText());
        JsonNode dimensionCount = alarmData.path("Dimensions").get(1);
        assertEquals("count", dimensionCount.get("Name").asText());
        assertEquals("2", dimensionCount.get("Value").asText());
    }

    @Test
    void getMetricStatistics_decimalEpochStartEndTime_filtersOutOfRangeDatapoints() {
        putMetric("NS", "M", "type", "current", 100.0, EPOCH_NOW);
        putMetric("NS", "M", "type", "old", 200.0, EPOCH_OLD);

        ObjectNode currentResult = getStats("NS", "M", "type", "current",
                EPOCH_NOW.minusSeconds(10), EPOCH_NOW.plusSeconds(10), 60);
        assertEquals(1, currentResult.get("Datapoints").size(),
                "current metric must be returned for a window around now");

        ObjectNode oldResult = getStats("NS", "M", "type", "old",
                EPOCH_NOW.minusSeconds(10), EPOCH_NOW.plusSeconds(10), 60);
        assertEquals(0, oldResult.get("Datapoints").size(),
                "metric from 24h ago must not be returned for a 20-second window around now");
    }

    // ──────────────────────────── Composite alarms ────────────────────────────

    private Response putCompositeAlarm(String name, String rule) {
        ObjectNode req = MAPPER.createObjectNode();
        req.put("AlarmName", name);
        req.put("AlarmRule", rule);
        req.put("AlarmDescription", "composite " + name);
        req.put("ActionsEnabled", true);
        req.putArray("AlarmActions").add("arn:aws:sns:us-east-1:000000000000:topic");
        req.putArray("InsufficientDataActions").add("arn:aws:sns:us-east-1:000000000000:topic");
        req.putArray("Tags").addObject().put("Key", "env").put("Value", "test");
        return handler.handle("PutCompositeAlarm", req, REGION);
    }

    private ObjectNode describeComposite(String name) {
        ObjectNode req = MAPPER.createObjectNode();
        req.putArray("AlarmNames").add(name);
        req.putArray("AlarmTypes").add("CompositeAlarm");
        Response resp = handler.handle("DescribeAlarms", req, REGION);
        assertEquals(200, resp.getStatus());
        return (ObjectNode) resp.getEntity();
    }

    @Test
    void putCompositeAlarm_thenDescribeWithAlarmTypes_returnsInsufficientDataComposite() {
        assertEquals(200, putCompositeAlarm("CompositeOne",
                "ALARM(\"MetricOne\") OR ALARM(\"MetricTwo\")").getStatus());

        ObjectNode body = describeComposite("CompositeOne");
        assertTrue(body.has("CompositeAlarms"), "AlarmTypes filter must yield a CompositeAlarms array");
        assertTrue(!body.has("MetricAlarms"), "composite-only filter must not emit MetricAlarms");
        JsonNode composite = body.get("CompositeAlarms").get(0);
        assertEquals("CompositeOne", composite.get("AlarmName").asText());
        assertEquals("ALARM(\"MetricOne\") OR ALARM(\"MetricTwo\")", composite.get("AlarmRule").asText());
        assertEquals("INSUFFICIENT_DATA", composite.get("StateValue").asText());
        assertTrue(composite.get("AlarmArn").asText().endsWith(":alarm:CompositeOne"));
        assertEquals("arn:aws:sns:us-east-1:000000000000:topic",
                composite.path("AlarmActions").get(0).asText());
        assertTrue(composite.get("StateUpdatedTimestamp").asLong() > 0);
    }

    @Test
    void describeAlarms_withoutAlarmTypes_reportsCompositeAndMetricTogether() {
        putCompositeAlarm("CompositeOne", "ALARM(\"MetricOne\")");
        ObjectNode putAlarmReq = MAPPER.createObjectNode();
        putAlarmReq.put("AlarmName", "MetricOne");
        putAlarmReq.put("MetricName", "M");
        putAlarmReq.put("Namespace", "NS");
        assertEquals(200, handler.handle("PutMetricAlarm", putAlarmReq, REGION).getStatus());

        ObjectNode req = MAPPER.createObjectNode();
        Response resp = handler.handle("DescribeAlarms", req, REGION);
        assertEquals(200, resp.getStatus());
        ObjectNode body = (ObjectNode) resp.getEntity();
        assertTrue(body.has("MetricAlarms"));
        assertEquals(1, body.get("MetricAlarms").size());
        assertTrue(body.has("CompositeAlarms"), "composite alarms must appear when types are unspecified");
        assertEquals(1, body.get("CompositeAlarms").size());
    }

    @Test
    void compositeAlarm_tagFamily_routesByArn() {
        putCompositeAlarm("CompositeOne", "ALARM(\"MetricOne\")");

        ObjectNode tagReq = MAPPER.createObjectNode();
        tagReq.put("ResourceARN", "arn:aws:cloudwatch:us-east-1:000000000000:alarm:CompositeOne");
        tagReq.putArray("Tags").addObject().put("Key", "team").put("Value", "platform");
        assertEquals(200, handler.handle("TagResource", tagReq, REGION).getStatus());

        ObjectNode listReq = MAPPER.createObjectNode();
        listReq.put("ResourceARN", "arn:aws:cloudwatch:us-east-1:000000000000:alarm:CompositeOne");
        Response listResp = handler.handle("ListTagsForResource", listReq, REGION);
        assertEquals(200, listResp.getStatus());
        JsonNode tags = ((ObjectNode) listResp.getEntity()).get("Tags");
        assertTrue(tags.isArray());
        assertEquals("env", tags.get(0).get("Key").asText());
        assertEquals("test", tags.get(0).get("Value").asText());
        assertEquals("team", tags.get(1).get("Key").asText());
        assertEquals("platform", tags.get(1).get("Value").asText());

        ObjectNode untagReq = MAPPER.createObjectNode();
        untagReq.put("ResourceARN", "arn:aws:cloudwatch:us-east-1:000000000000:alarm:CompositeOne");
        untagReq.putArray("TagKeys").add("team");
        assertEquals(200, handler.handle("UntagResource", untagReq, REGION).getStatus());

        Response after = handler.handle("ListTagsForResource", listReq, REGION);
        JsonNode remaining = ((ObjectNode) after.getEntity()).get("Tags");
        assertEquals(1, remaining.size());
        assertEquals("env", remaining.get(0).get("Key").asText());
    }

    @Test
    void setAlarmState_updatesCompositeAlarm() {
        putCompositeAlarm("CompositeOne", "ALARM(\"MetricOne\")");

        ObjectNode stateReq = MAPPER.createObjectNode();
        stateReq.put("AlarmName", "CompositeOne");
        stateReq.put("StateValue", "ALARM");
        stateReq.put("StateReason", "composite fired");
        assertEquals(200, handler.handle("SetAlarmState", stateReq, REGION).getStatus());

        JsonNode composite = describeComposite("CompositeOne").get("CompositeAlarms").get(0);
        assertEquals("ALARM", composite.get("StateValue").asText());
        assertEquals("composite fired", composite.get("StateReason").asText());
    }

    @Test
    void deleteAlarms_removesCompositeAlarm() {
        putCompositeAlarm("CompositeOne", "ALARM(\"MetricOne\")");

        ObjectNode deleteReq = MAPPER.createObjectNode();
        deleteReq.putArray("AlarmNames").add("CompositeOne");
        assertEquals(200, handler.handle("DeleteAlarms", deleteReq, REGION).getStatus());

        JsonNode body = describeComposite("CompositeOne");
        assertTrue(body.has("CompositeAlarms"));
        assertEquals(0, body.get("CompositeAlarms").size());
    }

    @Test
    void putCompositeAlarm_missingRule_rejected() {
        ObjectNode req = MAPPER.createObjectNode();
        req.put("AlarmName", "CompositeOne");
        // The handler surfaces the service's AwsException, which the JAX-RS exception mapper
        // turns into a 400 in the running emulator.
        assertThrows(io.github.hectorvent.floci.core.common.AwsException.class,
                () -> handler.handle("PutCompositeAlarm", req, REGION));
    }
}
