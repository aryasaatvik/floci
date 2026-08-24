package io.github.hectorvent.floci.services.s3;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Map;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;

@QuarkusTest
@TestProfile(S3SigV4PresignedUrlIntegrationTest.SignatureValidationProfile.class)
class S3SigV4PresignedUrlIntegrationTest {
    @Inject PreSignedUrlGenerator presignGenerator;

    @Test
    void signatureValidationAcceptsGeneratedCanonicalSigV4() {
        String bucket = "sigv4-presign-bucket";
        given().when().put("/" + bucket).then().statusCode(200);
        given().body("signed content").when().put("/" + bucket + "/object.txt").then().statusCode(200);
        String baseUrl = "http://localhost:" + io.restassured.RestAssured.port;
        URI presigned = URI.create(presignGenerator.generatePresignedUrl(baseUrl, bucket, "object.txt", "GET", 300));
        given().urlEncodingEnabled(false).when().get(presigned.getRawPath() + "?" + presigned.getRawQuery()).then().statusCode(200).body(equalTo("signed content"));
    }

    public static final class SignatureValidationProfile implements QuarkusTestProfile {
        @Override public Map<String, String> getConfigOverrides() {
            return Map.of("floci.auth.validate-signatures", "true", "floci.auth.presign-secret", "test-secret");
        }
    }
}
