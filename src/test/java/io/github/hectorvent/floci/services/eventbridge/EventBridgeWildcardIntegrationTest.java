package io.github.hectorvent.floci.services.eventbridge;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.hectorvent.floci.testing.RestAssuredJsonUtils;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.RestAssured;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.eventbridge.EventBridgeClient;
import software.amazon.awssdk.services.eventbridge.model.InvalidEventPatternException;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequestEntry;
import software.amazon.awssdk.services.eventbridge.model.Target;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
class EventBridgeWildcardIntegrationTest {

    private static final String SOURCE = "aws.guardduty";
    private static final String DETAIL_TYPE = "GuardDuty Malware Protection Object Scan Result";
    private static final String BUCKET = "history-imports";
    private static final String ACCOUNT = "741618800002";
    private static final String ORIGINAL = "org-1/emails/history-1/original.eml";
    private static final String ATTACHMENT = "org-2/emails/history-2/attachments/photo.jpg";
    private static final String ESCAPED_KEY = "org-3/emails/history-3/attachments/literal*\\.txt";
    private static final String UNRELATED = "org-1/emails/drafts/original.eml";

    @Inject
    ObjectMapper mapper;
    private EventBridgeClient events;

    @BeforeEach
    void connect() {
        RestAssuredJsonUtils.configureAwsContentTypes();
        URI endpoint = URI.create("http://localhost:" + RestAssured.port);
        StaticCredentialsProvider credentials = StaticCredentialsProvider.create(
                AwsBasicCredentials.create(ACCOUNT, "test"));
        events = EventBridgeClient.builder().endpointOverride(endpoint).region(Region.US_EAST_1)
                .httpClientBuilder(UrlConnectionHttpClient.builder()).credentialsProvider(credentials).build();
    }

    @AfterEach
    void close() {
        events.close();
    }

    @Test
    void guardDutyHistoryPatternAndControls() throws Exception {
        String wildcard = keyPattern(Map.of("wildcard", "*/emails/history-*"));
        assertTrue(matches(wildcard, BUCKET, ORIGINAL));
        assertTrue(matches(wildcard, BUCKET, ATTACHMENT));
        assertTrue(matches(wildcard, BUCKET, "/emails/history-"));
        assertFalse(matches(wildcard, "different-bucket", ORIGINAL));
        assertFalse(matches(wildcard, BUCKET, UNRELATED));
        assertFalse(matches(wildcard, BUCKET, "org-1/Emails/history-1"));

        String exact = keyPattern(ORIGINAL);
        assertTrue(matches(exact, BUCKET, ORIGINAL));
        assertFalse(matches(exact, BUCKET, ATTACHMENT));
        String prefix = keyPattern(Map.of("prefix", "org-2/emails/history-"));
        assertTrue(matches(prefix, BUCKET, ATTACHMENT));
        assertFalse(matches(prefix, BUCKET, ORIGINAL));
    }

    @Test
    void wildcardEscapesAnchoringAndStringTypes() throws Exception {
        String escaped = keyPattern(Map.of("wildcard", "*/literal\\*\\\\.txt"));
        assertTrue(matches(escaped, BUCKET, ESCAPED_KEY));
        assertFalse(matches(escaped, BUCKET, ESCAPED_KEY.replace("*", "x")));
        assertFalse(matches(escaped, BUCKET, ESCAPED_KEY + ".bak"));
        assertTrue(matches(keyPattern(Map.of("wildcard", "\\**")), BUCKET, "*suffix"));
        assertFalse(matches(keyPattern(Map.of("wildcard", "ab*bc")), BUCKET, "abc"));
        assertFalse(matches(keyPattern(Map.of("wildcard", "ab*x*bc")), BUCKET, "abc"));
        assertTrue(matches(keyPattern(Map.of("wildcard", "ab*x*bc")), BUCKET, "abxbc"));
        assertTrue(matches(keyPattern(Map.of("wildcard", "a*b")), BUCKET, "a\nb"));
        assertTrue(matches(keyPattern(Map.of("wildcard", "a.b")), BUCKET, "a.b"));
        assertFalse(matches(keyPattern(Map.of("wildcard", "a.b")), BUCKET, "axb"));
        String allStrings = keyPattern(Map.of("wildcard", "*"));
        assertTrue(matches(allStrings, BUCKET, ""));
        assertTrue(matches(allStrings, BUCKET, List.of(123, ORIGINAL)));
        assertFalse(matches(allStrings, BUCKET, 123));
        assertFalse(matches(allStrings, BUCKET, true));
        assertFalse(matches(allStrings, BUCKET, List.of(123, true)));
    }

    @Test
    void anythingButWildcardSupportsScalarAndList() throws Exception {
        String scalar = keyPattern(Map.of("anything-but", Map.of("wildcard", "*/attachments/*")));
        assertTrue(matches(scalar, BUCKET, ORIGINAL));
        assertFalse(matches(scalar, BUCKET, ATTACHMENT));
        assertFalse(matches(scalar, BUCKET, 123));

        String list = keyPattern(Map.of("anything-but",
                Map.of("wildcard", List.of("*/attachments/*", "*/drafts/*"))));
        assertTrue(matches(list, BUCKET, ORIGINAL));
        assertFalse(matches(list, BUCKET, ATTACHMENT));
        assertFalse(matches(list, BUCKET, UNRELATED));
    }

    @Test
    void invalidWildcardOperatorsAreRejectedByBothApis() throws Exception {
        for (Object operator : List.of(
                Map.of("wildcard", "org-**/emails"),
                Map.of("wildcard", "\\q"),
                Map.of("wildcard", "tail\\"),
                Map.of("wildcard", 123),
                Map.of("wildcard", List.of("*")),
                Map.of("wildcard", "*", "prefix", "org-1"),
                Map.of("anything-but", Map.of("wildcard", List.of("valid*", "**"))),
                Map.of("anything-but", Map.of("wildcard", List.of())))) {
            String pattern = keyPattern(operator);
            assertThrows(InvalidEventPatternException.class, () -> matches(pattern, BUCKET, ORIGINAL), pattern);
            assertThrows(InvalidEventPatternException.class,
                    () -> events.putRule(request -> request.name("invalid-wildcard").eventPattern(pattern)), pattern);
        }
        String invalidBranch = """
                {"$or":[{"source":["aws.guardduty"]},{"detail":{"key":[{"wildcard":"**"}]}}]}
                """;
        assertThrows(InvalidEventPatternException.class, () -> matches(invalidBranch, BUCKET, ORIGINAL));
        assertThrows(InvalidEventPatternException.class,
                () -> events.putRule(request -> request.name("invalid-wildcard-or").eventPattern(invalidBranch)));
    }

    @Test
    void putEventsRoutesOnlyMatchingKeysToSqs() throws Exception {
        String bus = "wildcard-history-test";
        events.createEventBus(request -> request.name(bus));
        try {
            List<PutEventsRequestEntry> entries = List.of(
                    entry(bus, BUCKET, ORIGINAL),
                    entry(bus, BUCKET, ATTACHMENT),
                    entry(bus, BUCKET, ESCAPED_KEY),
                    entry(bus, "different-bucket", ORIGINAL),
                    entry(bus, BUCKET, UNRELATED));
            assertDelivery(bus, "wildcard", Map.of("wildcard", "*/emails/history-*"),
                    entries, ORIGINAL, ATTACHMENT, ESCAPED_KEY);
            assertDelivery(bus, "exact", ORIGINAL, entries, ORIGINAL);
            assertDelivery(bus, "prefix", Map.of("prefix", "org-2/emails/history-"), entries, ATTACHMENT);
            assertDelivery(bus, "escaped", Map.of("wildcard", "*/literal\\*\\\\.txt"), entries, ESCAPED_KEY);
        } finally {
            events.deleteEventBus(request -> request.name(bus));
        }
    }

    private void assertDelivery(String bus, String rule, Object operator, List<PutEventsRequestEntry> entries,
                                String... expectedKeys) throws Exception {
        String queueUrl = sqs("CreateQueue", Map.of("QueueName", "eb-history-" + rule)).path("QueueUrl").textValue();
        try {
            String queueArn = sqs("GetQueueAttributes", Map.of("QueueUrl", queueUrl,
                    "AttributeNames", List.of("QueueArn"))).path("Attributes").path("QueueArn").textValue();
            String pattern = keyPattern(operator);
            events.putRule(request -> request.eventBusName(bus).name(rule).eventPattern(pattern));
            try {
                assertEquals(0, events.putTargets(request -> request.eventBusName(bus).rule(rule)
                        .targets(Target.builder().id("sqs").arn(queueArn).build())).failedEntryCount());
                assertEquals(0, events.putEvents(request -> request.entries(entries)).failedEntryCount());
                List<String> deliveredKeys = new ArrayList<>();
                JsonNode messages = sqs("ReceiveMessage", Map.of("QueueUrl", queueUrl, "MaxNumberOfMessages", 10));
                for (JsonNode message : messages.path("Messages")) {
                    JsonNode object = mapper.readTree(message.path("Body").textValue())
                            .path("detail").path("s3ObjectDetails");
                    assertEquals(BUCKET, object.path("bucketName").textValue());
                    deliveredKeys.add(object.path("objectKey").textValue());
                }
                assertEquals(List.of(expectedKeys).stream().sorted().toList(), deliveredKeys.stream().sorted().toList());
            } finally {
                events.removeTargets(request -> request.eventBusName(bus).rule(rule).ids("sqs"));
                events.deleteRule(request -> request.eventBusName(bus).name(rule));
            }
        } finally {
            sqs("DeleteQueue", Map.of("QueueUrl", queueUrl));
        }
    }

    private JsonNode sqs(String operation, Map<String, Object> request) throws Exception {
        String response = given().contentType("application/x-amz-json-1.0")
                .header("X-Amz-Target", "AmazonSQS." + operation)
                .header("Authorization", "AWS4-HMAC-SHA256 Credential=" + ACCOUNT + "/20261121/us-east-1/sqs/aws4_request")
                .body(mapper.writeValueAsString(request))
                .when().post("/").then().statusCode(200).extract().asString();
        return mapper.readTree(response);
    }

    private String keyPattern(Object operator) throws Exception {
        return mapper.writeValueAsString(Map.of("source", List.of(SOURCE), "detail-type", List.of(DETAIL_TYPE),
                "detail", Map.of("s3ObjectDetails", Map.of("bucketName", List.of(BUCKET),
                        "objectKey", List.of(operator)))));
    }

    private boolean matches(String pattern, String bucket, Object key) throws Exception {
        String event = mapper.writeValueAsString(Map.of("version", "0", "id", "sample", "source", SOURCE,
                "detail-type", DETAIL_TYPE, "account", ACCOUNT, "region", "us-east-1",
                "time", "2026-11-21T00:00:00Z", "resources", List.of(), "detail", detail(bucket, key)));
        return events.testEventPattern(request -> request.eventPattern(pattern).event(event)).result();
    }

    private PutEventsRequestEntry entry(String bus, String bucket, String key) throws Exception {
        return PutEventsRequestEntry.builder().eventBusName(bus).source(SOURCE).detailType(DETAIL_TYPE)
                .detail(mapper.writeValueAsString(detail(bucket, key))).build();
    }

    private Map<String, Object> detail(String bucket, Object key) {
        return Map.of("s3ObjectDetails", Map.of("bucketName", bucket, "objectKey", key));
    }
}
