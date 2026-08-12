package com.floci.test;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.apigatewayv2.ApiGatewayV2Client;
import software.amazon.awssdk.services.apigatewayv2.model.ApiGatewayV2Exception;
import software.amazon.awssdk.services.apigatewayv2.model.CreateApiMappingRequest;
import software.amazon.awssdk.services.apigatewayv2.model.CreateApiRequest;
import software.amazon.awssdk.services.apigatewayv2.model.CreateDomainNameRequest;
import software.amazon.awssdk.services.apigatewayv2.model.CreateStageRequest;
import software.amazon.awssdk.services.apigatewayv2.model.DeleteApiMappingRequest;
import software.amazon.awssdk.services.apigatewayv2.model.DeleteApiRequest;
import software.amazon.awssdk.services.apigatewayv2.model.DeleteDomainNameRequest;
import software.amazon.awssdk.services.apigatewayv2.model.DomainNameConfiguration;
import software.amazon.awssdk.services.apigatewayv2.model.EndpointType;
import software.amazon.awssdk.services.apigatewayv2.model.GetApiMappingRequest;
import software.amazon.awssdk.services.apigatewayv2.model.GetApiMappingsRequest;
import software.amazon.awssdk.services.apigatewayv2.model.GetDomainNameRequest;
import software.amazon.awssdk.services.apigatewayv2.model.GetTagsRequest;
import software.amazon.awssdk.services.apigatewayv2.model.NotFoundException;
import software.amazon.awssdk.services.apigatewayv2.model.ProtocolType;
import software.amazon.awssdk.services.apigatewayv2.model.RoutingMode;
import software.amazon.awssdk.services.apigatewayv2.model.SecurityPolicy;
import software.amazon.awssdk.services.apigatewayv2.model.TagResourceRequest;
import software.amazon.awssdk.services.apigatewayv2.model.UpdateApiMappingRequest;
import software.amazon.awssdk.services.apigatewayv2.model.UpdateDomainNameRequest;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("API Gateway v2 custom domains")
class ApiGatewayV2CustomDomainTest {

    private static ApiGatewayV2Client client;
    private static String apiId;
    private static String domainName;
    private static String domainArn;
    private static String apiMappingId;

    @BeforeAll
    static void setup() {
        client = TestFixtures.apiGatewayV2Client();
    }

    @AfterAll
    static void cleanup() {
        if (client == null) {
            return;
        }
        if (apiMappingId != null && domainName != null) {
            deleteIfPresent(() -> client.deleteApiMapping(DeleteApiMappingRequest.builder()
                    .domainName(domainName)
                    .apiMappingId(apiMappingId)
                    .build()));
        }
        if (domainName != null) {
            deleteIfPresent(() -> client.deleteDomainName(DeleteDomainNameRequest.builder()
                    .domainName(domainName)
                    .build()));
        }
        if (apiId != null) {
            deleteIfPresent(() -> client.deleteApi(DeleteApiRequest.builder().apiId(apiId).build()));
        }
        client.close();
    }

    @Test
    void customDomainAndApiMappingCrudUsesAwsSdkWireModel() {
        apiId = client.createApi(CreateApiRequest.builder()
                .name(TestFixtures.uniqueName("custom-domain-api"))
                .protocolType(ProtocolType.HTTP)
                .build()).apiId();
        client.createStage(CreateStageRequest.builder()
                .apiId(apiId)
                .stageName("$default")
                .autoDeploy(true)
                .build());

        domainName = TestFixtures.uniqueName("sdk-domain") + ".example.test";
        var createdDomain = client.createDomainName(CreateDomainNameRequest.builder()
                .domainName(domainName)
                .domainNameConfigurations(DomainNameConfiguration.builder()
                        .certificateArn("arn:aws:acm:us-east-1:123456789012:certificate/sdk-test")
                        .endpointType(EndpointType.REGIONAL)
                        .securityPolicy(SecurityPolicy.TLS_1_2)
                        .build())
                .tags(Map.of("owner", "sdk"))
                .build());

        domainArn = createdDomain.domainNameArn();
        assertThat(domainArn).isEqualTo("arn:aws:apigateway:us-east-1::/domainnames/" + domainName);
        assertThat(createdDomain.apiMappingSelectionExpression()).isEqualTo("$request.basepath");
        assertThat(createdDomain.routingMode()).isEqualTo(RoutingMode.API_MAPPING_ONLY);
        assertThat(createdDomain.domainNameConfigurations()).singleElement()
                .satisfies(configuration -> {
                    assertThat(configuration.apiGatewayDomainName()).isEqualTo(domainName + ".regional.local");
                    assertThat(configuration.domainNameStatusAsString()).isEqualTo("AVAILABLE");
                });

        assertThat(client.getDomainName(GetDomainNameRequest.builder().domainName(domainName).build()).tags())
                .containsEntry("owner", "sdk");
        assertThat(client.getDomainNames().items())
                .extracting(item -> item.domainName())
                .contains(domainName);

        var updatedDomain = client.updateDomainName(UpdateDomainNameRequest.builder()
                .domainName(domainName)
                .routingMode(RoutingMode.ROUTING_RULE_THEN_API_MAPPING)
                .build());
        assertThat(updatedDomain.routingMode()).isEqualTo(RoutingMode.ROUTING_RULE_THEN_API_MAPPING);

        client.tagResource(TagResourceRequest.builder()
                .resourceArn(domainArn)
                .tags(Map.of("environment", "compatibility"))
                .build());
        assertThat(client.getTags(GetTagsRequest.builder().resourceArn(domainArn).build()).tags())
                .containsEntry("owner", "sdk")
                .containsEntry("environment", "compatibility");

        var createdMapping = client.createApiMapping(CreateApiMappingRequest.builder()
                .domainName(domainName)
                .apiId(apiId)
                .stage("$default")
                .build());
        apiMappingId = createdMapping.apiMappingId();
        assertThat(apiMappingId).isNotBlank();

        assertThat(client.getApiMapping(GetApiMappingRequest.builder()
                .domainName(domainName)
                .apiMappingId(apiMappingId)
                .build()).stage()).isEqualTo("$default");
        assertThat(client.getApiMappings(GetApiMappingsRequest.builder().domainName(domainName).build()).items())
                .extracting(item -> item.apiMappingId())
                .contains(apiMappingId);

        var updatedMapping = client.updateApiMapping(UpdateApiMappingRequest.builder()
                .domainName(domainName)
                .apiMappingId(apiMappingId)
                .apiId(apiId)
                .apiMappingKey("v1")
                .build());
        assertThat(updatedMapping.apiMappingKey()).isEqualTo("v1");

        client.deleteApiMapping(DeleteApiMappingRequest.builder()
                .domainName(domainName)
                .apiMappingId(apiMappingId)
                .build());
        String deletedMappingId = apiMappingId;
        apiMappingId = null;
        assertThatThrownBy(() -> client.getApiMapping(GetApiMappingRequest.builder()
                .domainName(domainName)
                .apiMappingId(deletedMappingId)
                .build()))
                .isInstanceOf(NotFoundException.class);

        client.deleteDomainName(DeleteDomainNameRequest.builder().domainName(domainName).build());
        String deletedDomainName = domainName;
        domainName = null;
        assertThatThrownBy(() -> client.getDomainName(GetDomainNameRequest.builder()
                .domainName(deletedDomainName)
                .build()))
                .isInstanceOf(NotFoundException.class);
    }

    private static void deleteIfPresent(Runnable delete) {
        try {
            delete.run();
        } catch (ApiGatewayV2Exception error) {
            if (error.statusCode() != 404) {
                throw error;
            }
        }
    }
}
