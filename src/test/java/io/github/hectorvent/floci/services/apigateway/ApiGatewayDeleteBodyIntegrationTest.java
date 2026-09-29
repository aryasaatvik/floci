package io.github.hectorvent.floci.services.apigateway;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.hectorvent.floci.services.lambda.LambdaService;
import io.github.hectorvent.floci.services.lambda.model.InvocationType;
import io.github.hectorvent.floci.services.lambda.model.InvokeResult;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.nio.charset.StandardCharsets;

import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@QuarkusTest
class ApiGatewayDeleteBodyIntegrationTest {

    private static final String FUNCTION = "delete-body-rest-proxy";
    private static final String PATH = "/restapis/%s/test/_user_request_/items/42";
    private static final String JSON = "{\"remove\":true,\"count\":2}";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @InjectMock
    LambdaService lambdaService;

    private static String apiId;

    @BeforeEach
    void createRestProxyApi() {
        if (apiId != null) {
            return;
        }
        apiId = given().contentType(ContentType.JSON)
                .body("{\"name\":\"delete-body-regression\"}")
                .post("/restapis").then().statusCode(201).extract().path("id");
        String rootId = given().get("/restapis/" + apiId + "/resources")
                .then().statusCode(200).extract().path("item[0].id");
        String proxyId = given().contentType(ContentType.JSON)
                .body("{\"pathPart\":\"{proxy+}\"}")
                .post("/restapis/" + apiId + "/resources/" + rootId)
                .then().statusCode(201).extract().path("id");
        given().contentType(ContentType.JSON)
                .body("{\"authorizationType\":\"NONE\"}")
                .put("/restapis/" + apiId + "/resources/" + proxyId + "/methods/DELETE")
                .then().statusCode(201);
        String uri = "arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/"
                + "arn:aws:lambda:us-east-1:000000000000:function:" + FUNCTION + "/invocations";
        given().contentType(ContentType.JSON)
                .body("{\"type\":\"AWS_PROXY\",\"httpMethod\":\"POST\",\"uri\":\"" + uri + "\"}")
                .put("/restapis/" + apiId + "/resources/" + proxyId + "/methods/DELETE/integration")
                .then().statusCode(201);
        String deployment = given().contentType(ContentType.JSON).body("{}")
                .post("/restapis/" + apiId + "/deployments")
                .then().statusCode(201).extract().path("id");
        given().contentType(ContentType.JSON)
                .body("{\"stageName\":\"test\",\"deploymentId\":\"" + deployment + "\"}")
                .post("/restapis/" + apiId + "/stages").then().statusCode(201);
    }

    @BeforeEach
    void stubLambdaResponse() {
        reset(lambdaService);
        when(lambdaService.invoke(eq("us-east-1"), eq(FUNCTION), any(byte[].class),
                eq(InvocationType.RequestResponse)))
                .thenReturn(new InvokeResult(200, null,
                        "{\"statusCode\":200,\"body\":\"ok\"}".getBytes(StandardCharsets.UTF_8),
                        null, "delete-body-request"));
    }

    @Test
    void deleteBodyReachesLambdaThroughAllRestExecuteUrlShapes() throws Exception {
        for (String url : new String[]{
                "/execute-api/" + apiId + "/test/items/42",
                "/_aws/execute-api/" + apiId + "/test/items/42",
                PATH.formatted(apiId)}) {
            clearInvocations(lambdaService);
            given().contentType(ContentType.JSON).body(JSON).delete(url).then().statusCode(200);
            JsonNode event = invokedEvent();
            assertEquals(JSON, event.path("body").asText(), url);
            assertFalse(event.path("isBase64Encoded").asBoolean(), url);
        }
    }

    @Test
    void bodylessDeleteStillHasAnEmptyLambdaBody() throws Exception {
        given().delete("/execute-api/" + apiId + "/test/items/42").then().statusCode(200);
        JsonNode event = invokedEvent();
        assertTrue(event.path("body").isNull());
        assertFalse(event.path("isBase64Encoded").asBoolean());
    }

    private JsonNode invokedEvent() throws Exception {
        ArgumentCaptor<byte[]> payload = ArgumentCaptor.forClass(byte[].class);
        verify(lambdaService).invoke(eq("us-east-1"), eq(FUNCTION), payload.capture(),
                eq(InvocationType.RequestResponse));
        return MAPPER.readTree(payload.getValue());
    }
}
