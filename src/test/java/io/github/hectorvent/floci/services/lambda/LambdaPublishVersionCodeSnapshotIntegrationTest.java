package io.github.hectorvent.floci.services.lambda;

import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;

/** Proves PublishVersion returns the code snapshot fields an AWS client compares. */
@QuarkusTest
class LambdaPublishVersionCodeSnapshotIntegrationTest {

    private static final String BASE_PATH = "/2015-03-31";
    private static final String FUNCTION_NAME = "publish-version-code-snapshot";

    @Test
    void publishVersionCopiesZipCodeHashAndSizeIntoTheNumberedSnapshot() throws Exception {
        byte[] versionOneZip = lambdaZip(200);
        String zip = Base64.getEncoder().encodeToString(versionOneZip);
        try {
            given().contentType(ContentType.JSON)
                    .body("""
                            {
                              "FunctionName":"%s",
                              "Runtime":"nodejs20.x",
                              "Role":"arn:aws:iam::000000000000:role/lambda-role",
                              "Handler":"index.handler",
                              "Architectures":["arm64"],
                              "EphemeralStorage":{"Size":1024},
                              "TracingConfig":{"Mode":"Active"},
                              "Layers":["arn:aws:lambda:us-east-1:000000000000:layer:shared:1"],
                              "Code":{"ZipFile":"%s"}
                            }
                            """.formatted(FUNCTION_NAME, zip))
                    .when().post(BASE_PATH + "/functions")
                    .then().statusCode(201)
                    .body("Version", equalTo("$LATEST"))
                    .body("CodeSha256", notNullValue())
                    .body("CodeSize", greaterThan(0));

            String latestSha = given().when().get(BASE_PATH + "/functions/" + FUNCTION_NAME)
                    .then().statusCode(200)
                    .body("Configuration.CodeSha256", notNullValue())
                    .body("Configuration.CodeSize", greaterThan(0))
                    .extract().path("Configuration.CodeSha256");

            given().contentType(ContentType.JSON)
                    .body("{\"Description\":\"zip snapshot\"}")
                    .when().post(BASE_PATH + "/functions/" + FUNCTION_NAME + "/versions")
                    .then().statusCode(201)
                    .body("Version", equalTo("1"))
                    .body("CodeSha256", equalTo(latestSha))
                    .body("CodeSize", greaterThan(0))
                    .body("Architectures[0]", equalTo("arm64"))
                    .body("EphemeralStorage.Size", equalTo(1024))
                    .body("TracingConfig.Mode", equalTo("Active"))
                    .body("Layers[0].Arn", equalTo("arn:aws:lambda:us-east-1:000000000000:layer:shared:1"));

            given().queryParam("Qualifier", "1")
                    .when().get(BASE_PATH + "/functions/" + FUNCTION_NAME + "/configuration")
                    .then().statusCode(200)
                    .body("Version", equalTo("1"))
                    .body("CodeSha256", equalTo(latestSha));

            String versionOneLocation = given().queryParam("Qualifier", "1")
                    .when().get(BASE_PATH + "/functions/" + FUNCTION_NAME)
                    .then().statusCode(200)
                    .extract().path("Code.Location");
            byte[] latestZip = lambdaZip(201);
            given().contentType(ContentType.JSON)
                    .body("{\"ZipFile\":\"%s\"}".formatted(
                            Base64.getEncoder().encodeToString(latestZip)))
                    .when().put(BASE_PATH + "/functions/" + FUNCTION_NAME + "/code")
                    .then().statusCode(200);
            String latestLocation = given()
                    .when().get(BASE_PATH + "/functions/" + FUNCTION_NAME)
                    .then().statusCode(200)
                    .extract().path("Code.Location");

            assertNotEquals(versionOneLocation, latestLocation);
            assertArrayEquals(versionOneZip, download(versionOneLocation));
            assertArrayEquals(latestZip, download(latestLocation));
        } finally {
            given().when().delete(BASE_PATH + "/functions/" + FUNCTION_NAME);
        }
    }

    private static byte[] download(String location) {
        return given().when().get(java.net.URI.create(location).getRawPath())
                .then().statusCode(200).extract().asByteArray();
    }

    private static byte[] lambdaZip(int statusCode) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ZipOutputStream zip = new ZipOutputStream(bytes)) {
            zip.putNextEntry(new ZipEntry("index.js"));
            zip.write(("exports.handler = async () => ({ statusCode: " + statusCode + " });")
                    .getBytes(StandardCharsets.UTF_8));
            zip.closeEntry();
        }
        return bytes.toByteArray();
    }
}
