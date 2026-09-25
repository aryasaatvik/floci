package io.github.hectorvent.floci.services.lambda;

import io.github.hectorvent.floci.core.common.AwsException;
import io.github.hectorvent.floci.core.common.RegionResolver;
import io.github.hectorvent.floci.core.storage.InMemoryStorage;
import io.github.hectorvent.floci.services.lambda.model.AsyncInvokePolicy;
import io.github.hectorvent.floci.services.lambda.model.FunctionEventInvokeConfig;
import io.github.hectorvent.floci.services.lambda.model.LambdaFunction;
import io.github.hectorvent.floci.services.lambda.zip.CodeStore;
import io.github.hectorvent.floci.services.lambda.zip.ZipExtractor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class LambdaEventInvokeConfigTest {

    private static final String REGION = "us-east-1";

    private LambdaService service;

    @BeforeEach
    void setUp() {
        LambdaFunctionStore store = new LambdaFunctionStore(new InMemoryStorage<String, LambdaFunction>());
        WarmPool warmPool = new WarmPool();
        CodeStore codeStore = new CodeStore(Path.of("target/test-data/lambda-code"));
        RegionResolver regionResolver = new RegionResolver("us-east-1", "000000000000");
        service = new LambdaService(store, warmPool, codeStore, new ZipExtractor(), regionResolver);

        service.createFunction(REGION, Map.of(
                "FunctionName", "test-fn",
                "PackageType", "Image",
                "Role", "arn:aws:iam::000000000000:role/test-role",
                "Code", Map.of("ImageUri", "public.ecr.aws/lambda/nodejs:20")
        ));
    }

    @Test
    void putCreatesConfig() {
        Map<String, Object> req = new HashMap<>();
        req.put("MaximumRetryAttempts", 1);
        req.put("MaximumEventAgeInSeconds", 3600);

        FunctionEventInvokeConfig cfg = service.putEventInvokeConfig(REGION, "test-fn", null, req);

        assertEquals(1, cfg.getMaximumRetryAttempts());
        assertEquals(3600, cfg.getMaximumEventAgeInSeconds());
        assertTrue(cfg.getFunctionArn().endsWith(":$LATEST"));
        assertTrue(cfg.getLastModifiedSeconds() > 0);
    }

    @Test
    void putReplacesExistingConfig() {
        service.putEventInvokeConfig(REGION, "test-fn", null, Map.of("MaximumRetryAttempts", 2));

        Map<String, Object> req2 = new HashMap<>();
        req2.put("MaximumRetryAttempts", 0);
        FunctionEventInvokeConfig cfg = service.putEventInvokeConfig(REGION, "test-fn", null, req2);

        assertEquals(0, cfg.getMaximumRetryAttempts());
        assertNull(cfg.getMaximumEventAgeInSeconds());
    }

    @Test
    void getReturnsStoredConfig() {
        service.putEventInvokeConfig(REGION, "test-fn", null, Map.of("MaximumRetryAttempts", 1));

        FunctionEventInvokeConfig cfg = service.getEventInvokeConfig(REGION, "test-fn", null);

        assertEquals(1, cfg.getMaximumRetryAttempts());
    }

    @Test
    void getThrows404WhenNotFound() {
        AwsException ex = assertThrows(AwsException.class,
                () -> service.getEventInvokeConfig(REGION, "test-fn", null));
        assertEquals(404, ex.getHttpStatus());
    }

    @Test
    void updateMergesPartialFields() {
        service.putEventInvokeConfig(REGION, "test-fn", null, Map.of(
                "MaximumRetryAttempts", 2,
                "MaximumEventAgeInSeconds", 7200));

        Map<String, Object> partial = new HashMap<>();
        partial.put("MaximumRetryAttempts", 0);
        service.updateEventInvokeConfig(REGION, "test-fn", null, partial);

        FunctionEventInvokeConfig cfg = service.getEventInvokeConfig(REGION, "test-fn", null);
        assertEquals(0, cfg.getMaximumRetryAttempts());
        assertEquals(7200, cfg.getMaximumEventAgeInSeconds());
    }

    @Test
    void updateThrows404WhenNotFound() {
        AwsException ex = assertThrows(AwsException.class,
                () -> service.updateEventInvokeConfig(REGION, "test-fn", null, Map.of()));
        assertEquals(404, ex.getHttpStatus());
    }

    @Test
    void deleteRemovesConfig() {
        service.putEventInvokeConfig(REGION, "test-fn", null, Map.of("MaximumRetryAttempts", 1));
        service.deleteEventInvokeConfig(REGION, "test-fn", null);

        assertThrows(AwsException.class,
                () -> service.getEventInvokeConfig(REGION, "test-fn", null));
    }

    @Test
    void deleteThrows404WhenNotFound() {
        AwsException ex = assertThrows(AwsException.class,
                () -> service.deleteEventInvokeConfig(REGION, "test-fn", null));
        assertEquals(404, ex.getHttpStatus());
    }

    @Test
    void listReturnsAllConfigsForFunction() {
        service.putEventInvokeConfig(REGION, "test-fn", null, Map.of("MaximumRetryAttempts", 1));
        service.putEventInvokeConfig(REGION, "test-fn", "1", Map.of("MaximumRetryAttempts", 0));

        List<FunctionEventInvokeConfig> configs = service.listEventInvokeConfigs(REGION, "test-fn");

        assertEquals(2, configs.size());
    }

    @Test
    void putWithDestinationConfig() {
        Map<String, Object> req = new HashMap<>();
        req.put("DestinationConfig", Map.of(
                "OnSuccess", Map.of("Destination", "arn:aws:sqs:us-east-1:000000000000:my-queue"),
                "OnFailure", Map.of("Destination", "arn:aws:sqs:us-east-1:000000000000:dlq")
        ));

        FunctionEventInvokeConfig cfg = service.putEventInvokeConfig(REGION, "test-fn", null, req);

        assertNotNull(cfg.getDestinationConfig());
        assertEquals("arn:aws:sqs:us-east-1:000000000000:my-queue",
                cfg.getDestinationConfig().getOnSuccess().getDestination());
        assertEquals("arn:aws:sqs:us-east-1:000000000000:dlq",
                cfg.getDestinationConfig().getOnFailure().getDestination());
    }

    @Test
    void asyncPolicyUsesAwsDefaultsWithoutConfig() {
        LambdaFunction fn = service.getFunction(REGION, "test-fn");

        AsyncInvokePolicy policy = service.asyncInvokePolicy(REGION, fn, null);

        assertEquals(fn.getFunctionArn() + ":$LATEST", policy.invokedFunctionArn());
        assertEquals(2, policy.maximumRetryAttempts());
        assertEquals(21600, policy.maximumEventAgeSeconds());
        assertNull(policy.onFailureDestination());
        assertNull(policy.onSuccessDestination());
    }

    @Test
    void asyncPolicyAppliesUnqualifiedConfigToLatest() {
        service.putEventInvokeConfig(REGION, "test-fn", null, Map.of(
                "MaximumRetryAttempts", 1,
                "MaximumEventAgeInSeconds", 3600,
                "DestinationConfig", Map.of("OnFailure",
                        Map.of("Destination", "arn:aws:sqs:us-east-1:000000000000:dlq"))));
        LambdaFunction fn = service.getFunction(REGION, "test-fn");

        AsyncInvokePolicy policy = service.asyncInvokePolicy(REGION, fn, null);

        assertEquals(1, policy.maximumRetryAttempts());
        assertEquals(3600, policy.maximumEventAgeSeconds());
        assertEquals("arn:aws:sqs:us-east-1:000000000000:dlq", policy.onFailureDestination());
        assertEquals(policy, service.asyncInvokePolicy(REGION, fn, "$LATEST"));
    }

    @Test
    void asyncPolicyPrefersTheInvokedQualifiersConfig() {
        service.putEventInvokeConfig(REGION, "test-fn", null, Map.of("MaximumRetryAttempts", 2));
        service.putEventInvokeConfig(REGION, "test-fn", "live", Map.of(
                "MaximumRetryAttempts", 0,
                "DestinationConfig", Map.of("OnFailure",
                        Map.of("Destination", "arn:aws:sqs:us-east-1:000000000000:live-dlq"))));
        LambdaFunction version = publishedVersionSnapshot("7");

        AsyncInvokePolicy policy = service.asyncInvokePolicy(REGION, version, "live");

        assertEquals(service.getFunction(REGION, "test-fn").getFunctionArn() + ":live", policy.invokedFunctionArn());
        assertEquals(0, policy.maximumRetryAttempts());
        assertEquals("arn:aws:sqs:us-east-1:000000000000:live-dlq", policy.onFailureDestination());
    }

    @Test
    void asyncPolicyFallsBackToUnqualifiedConfigForAQualifierWithoutOne() {
        service.putEventInvokeConfig(REGION, "test-fn", null, Map.of(
                "MaximumRetryAttempts", 1,
                "DestinationConfig", Map.of("OnFailure",
                        Map.of("Destination", "arn:aws:sqs:us-east-1:000000000000:dlq"))));
        LambdaFunction version = publishedVersionSnapshot("7");

        AsyncInvokePolicy policy = service.asyncInvokePolicy(REGION, version, "7");

        assertEquals(service.getFunction(REGION, "test-fn").getFunctionArn() + ":7", policy.invokedFunctionArn());
        assertEquals(1, policy.maximumRetryAttempts());
        assertEquals("arn:aws:sqs:us-east-1:000000000000:dlq", policy.onFailureDestination());
    }

    private LambdaFunction publishedVersionSnapshot(String version) {
        LambdaFunction latest = service.getFunction(REGION, "test-fn");
        LambdaFunction snapshot = new LambdaFunction();
        snapshot.setFunctionName(latest.getFunctionName());
        snapshot.setFunctionArn(latest.getFunctionArn() + ":" + version);
        snapshot.setVersion(version);
        return snapshot;
    }
}
