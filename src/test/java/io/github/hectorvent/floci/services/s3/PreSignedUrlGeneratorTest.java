package io.github.hectorvent.floci.services.s3;

import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.MultivaluedMap;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PreSignedUrlGeneratorTest {
    private static final Clock FIXED_CLOCK = Clock.fixed(Instant.parse("2026-08-12T12:34:56Z"), ZoneOffset.UTC);
    private final PreSignedUrlGenerator generator = new PreSignedUrlGenerator("test-secret", 900, true, "us-east-1", "100000000001", FIXED_CLOCK);

    @Test
    void generatesCanonicalSigV4PresignedUrl() {
        String url = generator.generatePresignedUrl("https://api.local.test:4566/root", "bucket", "nested/object with spaces.txt", "GET", 300);
        assertEquals("https://api.local.test:4566/root/bucket/nested/object%20with%20spaces.txt"
                + "?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=100000000001%2F20260812%2Fus-east-1%2Fs3%2Faws4_request"
                + "&X-Amz-Date=20260812T123456Z&X-Amz-Expires=300&X-Amz-SignedHeaders=host"
                + "&X-Amz-Signature=bb5555e7f08c7196ad85580cbeabb464f7f0704f4ba4cecbf46136ecd517483c", url);
    }

    @Test
    void verifiesGeneratedSigV4PresignedUrl() {
        URI uri = URI.create(generator.generatePresignedUrl("https://api.local.test:4566", "bucket", "object.txt", "GET", 300));
        assertTrue(generator.verifySignature("GET", uri, host("api.local.test:4566")));
    }

    @Test
    void joinsEveryCanonicalHeaderValue() {
        MultivaluedMap<String, String> headers = host("api.local.test:4566");
        headers.add("X-Custom", " first   value ");
        headers.add("X-Custom", "second value");
        assertEquals("host:api.local.test:4566\nx-custom:first value,second value\n",
                PreSignedUrlGenerator.canonicalHeaders("host;x-custom", URI.create("https://api.local.test:4566/object"), headers));
    }

    private static MultivaluedMap<String, String> host(String value) {
        MultivaluedMap<String, String> headers = new MultivaluedHashMap<>();
        headers.add("Host", value);
        return headers;
    }
}
