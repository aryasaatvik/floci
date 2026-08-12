package io.github.hectorvent.floci.services.apigatewayv2;

import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.core.common.AwsException;
import io.github.hectorvent.floci.core.common.RegionResolver;
import io.github.hectorvent.floci.core.storage.AccountAwareStorageBackend;
import io.github.hectorvent.floci.core.storage.StorageFactory;
import io.github.hectorvent.floci.services.apigatewayv2.model.ApiMapping;
import io.github.hectorvent.floci.services.apigatewayv2.model.DomainName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ApiGatewayV2DomainNameServiceTest {

    private static final String REGION = "us-east-1";
    private static final String DOMAIN_NAME = "api.example.test";
    private static final String DOMAIN_ARN = "arn:aws:apigateway:us-east-1::/domainnames/" + DOMAIN_NAME;

    private StorageFactory storageFactory;
    private ApiGatewayV2Service service;

    @BeforeEach
    void setUp() {
        storageFactory = mockStorageFactory();
        service = newService(storageFactory);
    }

    @Test
    void domainAndMappingStateSurvivesServiceRecreation() {
        String apiId = createApiAndStage("$default");
        DomainName domain = service.createDomainName(REGION, domainRequest(Map.of("owner", "floci")));
        ApiMapping mapping = service.createApiMapping(REGION, DOMAIN_NAME,
                Map.of("apiId", apiId, "stage", "$default", "apiMappingKey", "v1"));

        ApiGatewayV2Service recreated = newService(storageFactory);

        assertEquals(domain.getDomainNameArn(), recreated.getDomainName(REGION, DOMAIN_NAME).getDomainNameArn());
        assertEquals("floci", recreated.getTags(DOMAIN_ARN).get("owner"));
        assertEquals(mapping.getApiMappingId(),
                recreated.getApiMapping(REGION, DOMAIN_NAME, mapping.getApiMappingId()).getApiMappingId());
    }

    @Test
    void deletingDomainCascadesMappings() {
        String apiId = createApiAndStage("prod");
        service.createDomainName(REGION, domainRequest(Map.of()));
        ApiMapping mapping = service.createApiMapping(REGION, DOMAIN_NAME,
                Map.of("apiId", apiId, "stage", "prod"));

        service.deleteDomainName(REGION, DOMAIN_NAME);

        assertAwsError("NotFoundException", 404,
                () -> service.getApiMapping(REGION, DOMAIN_NAME, mapping.getApiMappingId()));
        service.createDomainName(REGION, domainRequest(Map.of()));
        assertTrue(service.getApiMappings(REGION, DOMAIN_NAME).isEmpty());
    }

    @Test
    void mappingRequiresExistingApiAndStage() {
        String apiId = createApiAndStage("prod");
        service.createDomainName(REGION, domainRequest(Map.of()));

        assertAwsError("BadRequestException", 400,
                () -> service.createApiMapping(REGION, DOMAIN_NAME, Map.of("apiId", apiId)));
        assertAwsError("NotFoundException", 404,
                () -> service.createApiMapping(REGION, DOMAIN_NAME,
                        Map.of("apiId", apiId, "stage", "missing")));
        assertAwsError("NotFoundException", 404,
                () -> service.createApiMapping(REGION, DOMAIN_NAME,
                        Map.of("apiId", "missing", "stage", "prod")));
    }

    @Test
    void mappingKeysAreUniqueWithinDomain() {
        String apiId = createApiAndStage("prod");
        service.createDomainName(REGION, domainRequest(Map.of()));
        service.createApiMapping(REGION, DOMAIN_NAME,
                Map.of("apiId", apiId, "stage", "prod", "apiMappingKey", "v1"));

        assertAwsError("ConflictException", 409,
                () -> service.createApiMapping(REGION, DOMAIN_NAME,
                        Map.of("apiId", apiId, "stage", "prod", "apiMappingKey", "v1")));
    }

    @Test
    void updateMappingRequiresApiIdAndValidatesTargetStage() {
        String apiId = createApiAndStage("prod");
        service.createDomainName(REGION, domainRequest(Map.of()));
        ApiMapping mapping = service.createApiMapping(REGION, DOMAIN_NAME,
                Map.of("apiId", apiId, "stage", "prod"));

        assertAwsError("BadRequestException", 400,
                () -> service.updateApiMapping(REGION, DOMAIN_NAME, mapping.getApiMappingId(),
                        Map.of("apiMappingKey", "v1")));
        assertAwsError("NotFoundException", 404,
                () -> service.updateApiMapping(REGION, DOMAIN_NAME, mapping.getApiMappingId(),
                        Map.of("apiId", apiId, "stage", "missing")));
    }

    @Test
    void domainTagsMergeAndPersistIndependentlyFromApiTags() {
        service.createDomainName(REGION, domainRequest(Map.of("owner", "floci")));

        service.tagResource(DOMAIN_ARN, Map.of("environment", "test"));
        service.untagResource(DOMAIN_ARN, List.of("owner"));

        assertEquals(Map.of("environment", "test"), service.getTags(DOMAIN_ARN));
    }

    private String createApiAndStage(String stageName) {
        String apiId = service.createApi(REGION, Map.of("name", "domain-test", "protocolType", "HTTP")).getApiId();
        service.createStage(REGION, apiId, Map.of("stageName", stageName));
        return apiId;
    }

    private static Map<String, Object> domainRequest(Map<String, String> tags) {
        Map<String, Object> request = new HashMap<>();
        request.put("domainName", DOMAIN_NAME);
        request.put("domainNameConfigurations", List.of(Map.of(
                "certificateArn", "arn:aws:acm:us-east-1:123456789012:certificate/test",
                "endpointType", "REGIONAL",
                "securityPolicy", "TLS_1_2")));
        request.put("tags", tags);
        return request;
    }

    private static StorageFactory mockStorageFactory() {
        StorageFactory factory = mock(StorageFactory.class);
        Map<String, AccountAwareStorageBackend<?>> stores = new HashMap<>();
        when(factory.create(anyString(), anyString(), any())).thenAnswer(invocation ->
                stores.computeIfAbsent(invocation.getArgument(1),
                        ignored -> AccountAwareStorageBackend.inMemory("000000000000")));
        return factory;
    }

    private static ApiGatewayV2Service newService(StorageFactory factory) {
        return new ApiGatewayV2Service(factory, mock(EmulatorConfig.class),
                new RegionResolver(REGION, "000000000000"));
    }

    private static void assertAwsError(String errorCode, int status, Runnable operation) {
        AwsException error = assertThrows(AwsException.class, operation::run);
        assertEquals(errorCode, error.getErrorCode());
        assertEquals(status, error.getHttpStatus());
    }
}
