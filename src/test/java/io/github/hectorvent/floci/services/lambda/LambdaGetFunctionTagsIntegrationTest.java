package io.github.hectorvent.floci.services.lambda;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.io.ByteArrayOutputStream;
import java.util.Base64;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;

@QuarkusTest
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class LambdaGetFunctionTagsIntegrationTest {

    private static final String BASE_PATH = "/2015-03-31";
    private static final String TAGS_PATH = "/2017-03-31/tags/";
    private static final String FUNCTION_NAME = "get-function-tags-fn";
    private static final String FUNCTION_ARN =
            "arn:aws:lambda:us-east-1:000000000000:function:" + FUNCTION_NAME;

    @Test
    @Order(1)
    void getFunctionReturnsCreateTagsAtTheResponseRoot() throws Exception {
        given()
                .contentType("application/json")
                .body("""
                        {
                          "FunctionName": "%s",
                          "Runtime": "nodejs20.x",
                          "Role": "arn:aws:iam::000000000000:role/lambda-role",
                          "Handler": "index.handler",
                          "Code": { "ZipFile": "%s" },
                          "Tags": {
                            "alchemy::stack": "samva",
                            "alchemy::stage": "dev-test",
                            "alchemy::id": "Api"
                          }
                        }
                        """.formatted(FUNCTION_NAME, zip("exports.handler = async () => ({});")))
                .when()
                .post(BASE_PATH + "/functions")
                .then()
                .statusCode(201);

        assertGetFunctionTags("");
        assertListTags();
    }

    @Test
    @Order(2)
    void configurationAndCodeUpdatesPreserveGetFunctionTags() throws Exception {
        given()
                .contentType("application/json")
                .body("{\"Timeout\": 9}")
                .when()
                .put(BASE_PATH + "/functions/" + FUNCTION_NAME + "/configuration")
                .then()
                .statusCode(200)
                .body("Timeout", equalTo(9));

        assertGetFunctionTags("");

        given()
                .contentType("application/json")
                .body("{\"ZipFile\": \"%s\"}"
                        .formatted(zip("exports.handler = async () => ({ updated: true });")))
                .when()
                .put(BASE_PATH + "/functions/" + FUNCTION_NAME + "/code")
                .then()
                .statusCode(200);

        assertGetFunctionTags("");
        assertListTags();
    }

    @Test
    @Order(3)
    void qualifiedGetFunctionReturnsFunctionLevelTags() {
        given()
                .contentType("application/json")
                .body("{}")
                .when()
                .post(BASE_PATH + "/functions/" + FUNCTION_NAME + "/versions")
                .then()
                .statusCode(201)
                .body("Version", equalTo("1"));

        assertGetFunctionTags("?Qualifier=1");
        assertListTags();
    }

    private static void assertGetFunctionTags(String query) {
        given()
                .when()
                .get(BASE_PATH + "/functions/" + FUNCTION_NAME + query)
                .then()
                .statusCode(200)
                .body("Tags.'alchemy::stack'", equalTo("samva"))
                .body("Tags.'alchemy::stage'", equalTo("dev-test"))
                .body("Tags.'alchemy::id'", equalTo("Api"));
    }

    private static void assertListTags() {
        given()
                .when()
                .get(TAGS_PATH + FUNCTION_ARN)
                .then()
                .statusCode(200)
                .body("Tags.'alchemy::stack'", equalTo("samva"))
                .body("Tags.'alchemy::stage'", equalTo("dev-test"))
                .body("Tags.'alchemy::id'", equalTo("Api"));
    }

    private static String zip(String source) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ZipOutputStream zip = new ZipOutputStream(bytes)) {
            zip.putNextEntry(new ZipEntry("index.js"));
            zip.write(source.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            zip.closeEntry();
        }
        return Base64.getEncoder().encodeToString(bytes.toByteArray());
    }
}
