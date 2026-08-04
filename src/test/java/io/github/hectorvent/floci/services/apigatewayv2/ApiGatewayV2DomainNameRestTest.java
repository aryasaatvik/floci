package io.github.hectorvent.floci.services.apigatewayv2;

import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.notNullValue;

/**
 * REST compatibility coverage for the API Gateway v2 custom-domain and API-mapping
 * operations used by the AWS SDK.  This intentionally excludes routing rules and
 * data-plane custom-domain routing: it verifies the control-plane surface only.
 */
@QuarkusTest
class ApiGatewayV2DomainNameRestTest {

    @Test
    void customDomainAndApiMappingLifecycleMatchesAwsRestShapes() {
        String apiId = createApi();
        String domainName = "api-domain-rest-test.example.test";
        String domainPath = "/v2/domainnames/" + domainName;
        String domainArn = "arn:aws:apigateway:us-east-1::/domainnames/" + domainName;

        createDefaultStage(apiId);

        given()
                .contentType(ContentType.JSON)
                .body("""
                        {"domainName":"%s","domainNameConfigurations":[{
                          "certificateArn":"arn:aws:acm:us-east-1:123456789012:certificate/test",
                          "endpointType":"REGIONAL","securityPolicy":"TLS_1_2"
                        }],"routingMode":"API_MAPPING_ONLY","tags":{"owner":"samva"}}
                        """.formatted(domainName))
                .when().post("/v2/domainnames")
                .then().statusCode(201)
                .body("domainName", equalTo(domainName))
                .body("domainNameArn", equalTo(domainArn))
                .body("domainNameConfigurations[0].certificateArn", equalTo("arn:aws:acm:us-east-1:123456789012:certificate/test"))
                .body("domainNameConfigurations[0].endpointType", equalTo("REGIONAL"))
                .body("domainNameConfigurations[0].securityPolicy", equalTo("TLS_1_2"))
                .body("domainNameConfigurations[0].apiGatewayDomainName", equalTo(domainName + ".regional.local"))
                .body("routingMode", equalTo("API_MAPPING_ONLY"));

        given().when().get(domainPath)
                .then().statusCode(200)
                .body("domainName", equalTo(domainName));
        given().when().get("/v2/domainnames")
                .then().statusCode(200)
                .body("items.domainName", hasItem(domainName));

        given().contentType(ContentType.JSON)
                .body("{\"routingMode\":\"ROUTING_RULE_THEN_API_MAPPING\"}")
                .when().patch(domainPath)
                .then().statusCode(200)
                .body("routingMode", equalTo("ROUTING_RULE_THEN_API_MAPPING"));

        given().contentType(ContentType.JSON)
                .body("{\"tags\":{\"environment\":\"local\"}}")
                .when().post("/v2/tags/" + domainArn)
                .then().statusCode(201);
        given().when().get("/v2/tags/" + domainArn)
                .then().statusCode(200)
                .body("tags.owner", equalTo("samva"))
                .body("tags.environment", equalTo("local"));

        String mappingId = given().contentType(ContentType.JSON)
                .body("{\"apiId\":\"%s\",\"stage\":\"$default\"}".formatted(apiId))
                .when().post(domainPath + "/apimappings")
                .then().statusCode(201)
                .body("apiMappingId", notNullValue())
                .body("apiId", equalTo(apiId))
                .body("stage", equalTo("$default"))
                .extract().path("apiMappingId");

        String mappingPath = domainPath + "/apimappings/" + mappingId;
        given().when().get(mappingPath)
                .then().statusCode(200)
                .body("apiMappingId", equalTo(mappingId));
        given().when().get(domainPath + "/apimappings")
                .then().statusCode(200)
                .body("items.apiMappingId", hasItem(mappingId));

        given().contentType(ContentType.JSON)
                .body("{\"apiMappingKey\":\"v1\"}")
                .when().patch(mappingPath)
                .then().statusCode(200)
                .body("apiMappingKey", equalTo("v1"));

        given().when().delete(mappingPath).then().statusCode(204);
        given().when().get(mappingPath).then().statusCode(404);
        given().when().delete(domainPath).then().statusCode(204);
        given().when().get(domainPath).then().statusCode(404);
        given().when().delete("/v2/apis/" + apiId).then().statusCode(204);
    }

    private static String createApi() {
        return given().contentType(ContentType.JSON)
                .body("{\"name\":\"domain-rest-api\",\"protocolType\":\"HTTP\"}")
                .when().post("/v2/apis")
                .then().statusCode(201)
                .extract().path("apiId");
    }

    private static void createDefaultStage(String apiId) {
        given().contentType(ContentType.JSON)
                .body("{\"stageName\":\"$default\",\"autoDeploy\":true}")
                .when().post("/v2/apis/" + apiId + "/stages")
                .then().statusCode(201);
    }
}
