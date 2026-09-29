package io.github.hectorvent.floci.services.apigatewayv2;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.hectorvent.floci.services.lambda.LambdaService;
import io.github.hectorvent.floci.services.lambda.model.InvocationType;
import io.github.hectorvent.floci.services.lambda.model.InvokeResult;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import io.restassured.RestAssured;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@QuarkusTest
class ApiGatewayV2DeleteBodyIntegrationTest {

    private static final String FUNCTION = "delete-body-http-api-v2";
    private static final String JSON = "{\"remove\":true,\"count\":2}";
    private static final byte[] BINARY = {(byte) 0xff, (byte) 0x00, (byte) 0x7f};
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @InjectMock
    LambdaService lambdaService;

    private static String apiId;

    @BeforeEach
    void createHttpApi() {
        if (apiId != null) {
            return;
        }
        apiId = given().contentType(ContentType.JSON)
                .body("{\"name\":\"delete-body-http-v2\",\"protocolType\":\"HTTP\"}")
                .post("/v2/apis").then().statusCode(201).extract().path("apiId");
        String integrationId = given().contentType(ContentType.JSON)
                .body("{\"integrationType\":\"AWS_PROXY\",\"integrationUri\":\"arn:aws:lambda:us-east-1:000000000000:function:"
                        + FUNCTION + "\",\"integrationMethod\":\"POST\",\"payloadFormatVersion\":\"2.0\"}")
                .post("/v2/apis/" + apiId + "/integrations")
                .then().statusCode(201).extract().path("integrationId");
        given().contentType(ContentType.JSON)
                .body("{\"routeKey\":\"DELETE /items/{id}\",\"target\":\"integrations/"
                        + integrationId + "\"}")
                .post("/v2/apis/" + apiId + "/routes").then().statusCode(201);
        given().contentType(ContentType.JSON)
                .body("{\"stageName\":\"test\",\"autoDeploy\":true}")
                .post("/v2/apis/" + apiId + "/stages").then().statusCode(201);
    }

    @BeforeEach
    void stubLambdaResponse() {
        reset(lambdaService);
        when(lambdaService.invoke(eq("us-east-1"), eq(FUNCTION), any(byte[].class),
                eq(InvocationType.RequestResponse)))
                .thenReturn(new InvokeResult(200, null,
                        "{\"statusCode\":200,\"body\":\"ok\"}".getBytes(StandardCharsets.UTF_8),
                        null, "delete-body-v2-request"));
    }

    @Test
    void jsonDeleteBodyReachesHttpApiV2LambdaEvent() throws Exception {
        given().contentType(ContentType.JSON).body(JSON)
                .delete("/execute-api/" + apiId + "/test/items/42").then().statusCode(200);
        JsonNode event = invokedEvent();
        assertEquals(JSON, event.path("body").asText());
        assertFalse(event.path("isBase64Encoded").asBoolean());
        assertEquals("DELETE", event.path("requestContext").path("http").path("method").asText());
    }

    @Test
    void binaryDeleteBodyIsBase64InHttpApiV2LambdaEvent() throws Exception {
        given().contentType("application/octet-stream").body(BINARY)
                .delete("/execute-api/" + apiId + "/test/items/42").then().statusCode(200);
        JsonNode event = invokedEvent();
        assertEquals(Base64.getEncoder().encodeToString(BINARY), event.path("body").asText());
        assertTrue(event.path("isBase64Encoded").asBoolean());
    }

    @Test
    void deleteBodyReachesLambdaThroughExecuteApiHostnameRoute() throws Exception {
        given().header("Host", apiId + ".execute-api.localhost.floci.io:" + RestAssured.port)
                .contentType(ContentType.JSON).body(JSON)
                .delete("/test/items/42").then().statusCode(200);
        JsonNode event = invokedEvent();
        assertEquals(JSON, event.path("body").asText());
        assertFalse(event.path("isBase64Encoded").asBoolean());
    }

    private JsonNode invokedEvent() throws Exception {
        ArgumentCaptor<byte[]> payload = ArgumentCaptor.forClass(byte[].class);
        verify(lambdaService).invoke(eq("us-east-1"), eq(FUNCTION), payload.capture(),
                eq(InvocationType.RequestResponse));
        return MAPPER.readTree(payload.getValue());
    }
}
