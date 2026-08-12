package io.github.hectorvent.floci.services.apigatewayv2;

import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.notNullValue;

@QuarkusTest
class ApiGatewayV2DomainNameRestTest {

    @Test
    void customDomainAndApiMappingLifecycleMatchesAwsRestShapes() {
        String suffix = UUID.randomUUID().toString().substring(0, 8);
        String apiId = createApi(suffix);
        String domainName = "api-" + suffix + ".example.test";
        String domainPath = "/v2/domainnames/" + domainName;
        String domainArn = "arn:aws:apigateway:us-east-1::/domainnames/" + domainName;

        createStage(apiId, "$default");

        given()
                .contentType(ContentType.JSON)
                .body("""
                        {"domainName":"%s","domainNameConfigurations":[{
                          "certificateArn":"arn:aws:acm:us-east-1:123456789012:certificate/test",
                          "endpointType":"REGIONAL","securityPolicy":"TLS_1_2"
                        }],"tags":{"owner":"floci"}}
                        """.formatted(domainName))
                .when().post("/v2/domainnames")
                .then().statusCode(201)
                .body("domainName", equalTo(domainName))
                .body("domainNameArn", equalTo(domainArn))
                .body("apiMappingSelectionExpression", equalTo("$request.basepath"))
                .body("routingMode", equalTo("API_MAPPING_ONLY"))
                .body("domainNameConfigurations[0].apiGatewayDomainName", equalTo(domainName + ".regional.local"))
                .body("domainNameConfigurations[0].domainNameStatus", equalTo("AVAILABLE"));

        given().when().get(domainPath)
                .then().statusCode(200)
                .body("tags.owner", equalTo("floci"));
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
                .body("tags.owner", equalTo("floci"))
                .body("tags.environment", equalTo("local"));

        given().contentType(ContentType.JSON)
                .body("{\"apiId\":\"%s\",\"stage\":\"missing\"}".formatted(apiId))
                .when().post(domainPath + "/apimappings")
                .then().statusCode(404);

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
                .body("{\"apiId\":\"%s\",\"apiMappingKey\":\"v1\"}".formatted(apiId))
                .when().patch(mappingPath)
                .then().statusCode(200)
                .body("apiMappingKey", equalTo("v1"));

        given().when().delete(mappingPath).then().statusCode(204);
        given().when().get(mappingPath).then().statusCode(404);
        given().when().delete(domainPath).then().statusCode(204);
        given().when().get(domainPath).then().statusCode(404);
        given().when().delete("/v2/apis/" + apiId).then().statusCode(204);
    }

    private static String createApi(String suffix) {
        return given().contentType(ContentType.JSON)
                .body("{\"name\":\"domain-rest-%s\",\"protocolType\":\"HTTP\"}".formatted(suffix))
                .when().post("/v2/apis")
                .then().statusCode(201)
                .extract().path("apiId");
    }

    private static void createStage(String apiId, String stageName) {
        given().contentType(ContentType.JSON)
                .body("{\"stageName\":\"%s\",\"autoDeploy\":true}".formatted(stageName))
                .when().post("/v2/apis/" + apiId + "/stages")
                .then().statusCode(201);
    }
}
