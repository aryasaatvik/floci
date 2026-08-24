package io.github.hectorvent.floci.services.s3;

import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.core.common.RequestContext;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.ContextNotActiveException;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.MultivaluedMap;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

@ApplicationScoped
public class PreSignedUrlGenerator {

    private static final DateTimeFormatter AMZ_DATE_FORMAT =
            DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneOffset.UTC);

    private final String secret;
    private final int defaultExpiry;
    private final boolean validateSignatures;
    private final String defaultRegion;
    private final String defaultAccountId;
    private final Clock clock;

    // Field-injected so the package-private test constructors remain valid
    @Inject
    Instance<RequestContext> requestContextInstance;

    @Inject
    public PreSignedUrlGenerator(EmulatorConfig config) {
        this(config.auth().presignSecret(),
             config.services().s3().defaultPresignExpirySeconds(),
             config.auth().validateSignatures(),
             config.defaultRegion(),
             config.defaultAccountId());
    }

    /** Package-private constructor for testing. */
    PreSignedUrlGenerator(String secret, int defaultExpiry) {
        this(secret, defaultExpiry, false, "us-east-1", "000000000000");
    }

    PreSignedUrlGenerator(String secret, int defaultExpiry, boolean validateSignatures) {
        this(secret, defaultExpiry, validateSignatures, "us-east-1", "000000000000");
    }

    PreSignedUrlGenerator(String secret, int defaultExpiry, boolean validateSignatures, String defaultRegion) {
        this(secret, defaultExpiry, validateSignatures, defaultRegion, "000000000000");
    }

    PreSignedUrlGenerator(String secret, int defaultExpiry, boolean validateSignatures, String defaultRegion, String defaultAccountId) {
        this(secret, defaultExpiry, validateSignatures, defaultRegion, defaultAccountId, Clock.systemUTC());
    }

    PreSignedUrlGenerator(String secret, int defaultExpiry, boolean validateSignatures, String defaultRegion,
                          String defaultAccountId, Clock clock) {
        this.secret = secret;
        this.defaultExpiry = defaultExpiry;
        this.validateSignatures = validateSignatures;
        this.defaultRegion = defaultRegion;
        this.defaultAccountId = defaultAccountId;
        this.clock = clock;
    }

    private String resolveAccessKeyId() {
        if (requestContextInstance != null) {
            try {
                String accountId = requestContextInstance.get().getAccountId();
                if (accountId != null) {
                    return accountId;
                }
            } catch (ContextNotActiveException ignored) {
                // outside request scope — fall through to default
            }
        }
        return defaultAccountId;
    }

    public boolean shouldValidateSignatures() {
        return validateSignatures;
    }

    public String generatePresignedUrl(String baseUrl, String bucket, String key,
                                         String method, int expiresSeconds) {
        int expiry = expiresSeconds > 0 ? expiresSeconds : defaultExpiry;
        String amzDate = AMZ_DATE_FORMAT.format(clock.instant());
        String credential = resolveAccessKeyId() + "/" + amzDate.substring(0, 8) + "/" + defaultRegion + "/s3/aws4_request";
        URI baseUri = URI.create(baseUrl);
        String canonicalUri = canonicalUri(baseUri.getRawPath(), bucket, key);
        String host = canonicalHost(baseUri);
        List<QueryParameter> queryParameters = List.of(
                new QueryParameter("X-Amz-Algorithm", "AWS4-HMAC-SHA256"),
                new QueryParameter("X-Amz-Credential", credential),
                new QueryParameter("X-Amz-Date", amzDate),
                new QueryParameter("X-Amz-Expires", Integer.toString(expiry)),
                new QueryParameter("X-Amz-SignedHeaders", "host"));
        String canonicalQuery = canonicalQueryString(queryParameters);
        String credentialScope = credential.substring(credential.indexOf('/') + 1);
        String signature = sign(method, canonicalUri, canonicalQuery, "host:" + host + "\n", "host",
                "UNSIGNED-PAYLOAD", amzDate, credentialScope);

        String origin = baseUri.getScheme() + "://" + baseUri.getRawAuthority();
        return origin + canonicalUri
                + "?" + canonicalQuery + "&X-Amz-Signature=" + signature;
    }

    public boolean isExpired(String amzDate, int expiresSeconds) {
        try {
            Instant signedAt = Instant.from(AMZ_DATE_FORMAT.parse(amzDate));
            return clock.instant().isAfter(signedAt.plusSeconds(expiresSeconds));
        } catch (Exception e) {
            return true;
        }
    }

    public boolean verifySignature(String method, URI requestUri,
                                   MultivaluedMap<String, String> headers) {
        try {
            List<QueryParameter> queryParameters = parseRawQuery(requestUri.getRawQuery());
            String algorithm = queryValue(queryParameters, "X-Amz-Algorithm");
            String credential = queryValue(queryParameters, "X-Amz-Credential");
            String amzDate = queryValue(queryParameters, "X-Amz-Date");
            String signature = queryValue(queryParameters, "X-Amz-Signature");
            String signedHeaders = queryValue(queryParameters, "X-Amz-SignedHeaders");

            if (!"AWS4-HMAC-SHA256".equals(algorithm) || credential == null || amzDate == null
                    || signature == null || signedHeaders == null) {
                return false;
            }

            String[] credentialParts = credential.split("/", -1);
            if (credentialParts.length != 5 || credentialParts[0].isBlank()
                    || !credentialParts[1].equals(amzDate.substring(0, 8))
                    || !"s3".equals(credentialParts[3]) || !"aws4_request".equals(credentialParts[4])) {
                return false;
            }

            String canonicalHeaders = canonicalHeaders(signedHeaders, requestUri, headers);
            if (canonicalHeaders == null) {
                return false;
            }
            String canonicalQuery = canonicalQueryString(queryParameters.stream()
                    .filter(parameter -> !"X-Amz-Signature".equals(parameter.name())).toList());
            String credentialScope = String.join("/", credentialParts[1], credentialParts[2],
                    credentialParts[3], credentialParts[4]);
            String expected = sign(method, canonicalRequestPath(requestUri), canonicalQuery, canonicalHeaders,
                    signedHeaders, "UNSIGNED-PAYLOAD", amzDate, credentialScope);
            return MessageDigest.isEqual(expected.getBytes(StandardCharsets.UTF_8),
                    signature.getBytes(StandardCharsets.UTF_8));
        } catch (Exception ignored) {
            return false;
        }
    }

    private String sign(String method, String canonicalUri, String canonicalQuery, String canonicalHeaders,
                        String signedHeaders, String payloadHash, String amzDate, String credentialScope) {
        String canonicalRequest = method + "\n" + canonicalUri + "\n" + canonicalQuery + "\n"
                + canonicalHeaders + "\n"
                + signedHeaders + "\n" + payloadHash;
        String stringToSign = "AWS4-HMAC-SHA256\n" + amzDate + "\n" + credentialScope + "\n"
                + sha256Hex(canonicalRequest);
        String[] scope = credentialScope.split("/", -1);
        byte[] signingKey = deriveSigningKey(secret, scope[0], scope[1], scope[2]);
        return hexEncode(hmacSha256(signingKey, stringToSign));
    }

    private static String canonicalUri(String basePath, String bucket, String key) {
        String prefix = basePath == null || basePath.isEmpty() || "/".equals(basePath)
                ? "" : (basePath.startsWith("/") ? basePath : "/" + basePath);
        return prefix + "/" + uriEncode(bucket) + "/" + uriEncodePath(key);
    }

    private static String canonicalRequestPath(URI requestUri) {
        String path = requestUri.getRawPath();
        return path == null || path.isEmpty() ? "/" : path;
    }

    private static String canonicalHost(URI uri) {
        String host = uri.getHost();
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("Pre-signed URL base URL must include a host.");
        }
        int port = uri.getPort();
        boolean defaultPort = port == -1 || ("http".equalsIgnoreCase(uri.getScheme()) && port == 80)
                || ("https".equalsIgnoreCase(uri.getScheme()) && port == 443);
        return defaultPort ? host : host + ":" + port;
    }

    static String canonicalHeaders(String signedHeaders, URI requestUri,
                                   MultivaluedMap<String, String> headers) {
        StringBuilder canonical = new StringBuilder();
        for (String headerName : signedHeaders.split(";", -1)) {
            String normalizedName = headerName.toLowerCase(Locale.ROOT);
            if (normalizedName.isBlank() || !normalizedName.equals(headerName)) {
                return null;
            }
            List<String> values = headerValues(normalizedName, requestUri, headers);
            if (values.isEmpty()) {
                return null;
            }
            canonical.append(normalizedName).append(':')
                    .append(String.join(",", values.stream()
                            .map(PreSignedUrlGenerator::normalizeHeaderValue)
                            .toList()))
                    .append('\n');
        }
        return canonical.toString();
    }

    private static List<String> headerValues(String name, URI requestUri,
                                             MultivaluedMap<String, String> headers) {
        for (Map.Entry<String, List<String>> entry : headers.entrySet()) {
            if (name.equalsIgnoreCase(entry.getKey()) && !entry.getValue().isEmpty()) {
                return entry.getValue();
            }
        }
        return "host".equals(name) ? List.of(canonicalHost(requestUri)) : List.of();
    }

    private static String normalizeHeaderValue(String value) {
        return value.trim().replaceAll("\\s+", " ");
    }

    private static List<QueryParameter> parseRawQuery(String raw) {
        if (raw == null || raw.isEmpty()) {
            return List.of();
        }
        List<QueryParameter> parameters = new ArrayList<>();
        for (String pair : raw.split("&", -1)) {
            int separator = pair.indexOf('=');
            String rawName = separator >= 0 ? pair.substring(0, separator) : pair;
            String rawValue = separator >= 0 ? pair.substring(separator + 1) : "";
            parameters.add(new QueryParameter(percentDecode(rawName), percentDecode(rawValue)));
        }
        return parameters;
    }

    private static String queryValue(List<QueryParameter> parameters, String name) {
        return parameters.stream().filter(parameter -> name.equals(parameter.name()))
                .map(QueryParameter::value).findFirst().orElse(null);
    }

    private static String canonicalQueryString(List<QueryParameter> parameters) {
        return parameters.stream()
                .map(parameter -> new QueryParameter(uriEncode(parameter.name()), uriEncode(parameter.value())))
                .sorted(Comparator.comparing(QueryParameter::name).thenComparing(QueryParameter::value))
                .map(parameter -> parameter.name() + "=" + parameter.value())
                .reduce((left, right) -> left + "&" + right)
                .orElse("");
    }

    private static String uriEncodePath(String value) {
        return java.util.Arrays.stream(value.split("/", -1))
                .map(PreSignedUrlGenerator::uriEncode)
                .reduce((left, right) -> left + "/" + right)
                .orElse("");
    }

    private static String uriEncode(String value) {
        StringBuilder encoded = new StringBuilder();
        for (byte valueByte : value.getBytes(StandardCharsets.UTF_8)) {
            int unsigned = valueByte & 0xff;
            if ((unsigned >= 'A' && unsigned <= 'Z') || (unsigned >= 'a' && unsigned <= 'z')
                    || (unsigned >= '0' && unsigned <= '9') || unsigned == '-' || unsigned == '_'
                    || unsigned == '.' || unsigned == '~') {
                encoded.append((char) unsigned);
            } else {
                encoded.append('%').append(String.format("%02X", unsigned));
            }
        }
        return encoded.toString();
    }

    private static String percentDecode(String value) {
        byte[] bytes = new byte[value.length() * 3];
        int length = 0;
        for (int index = 0; index < value.length(); index++) {
            char current = value.charAt(index);
            if (current == '%' && index + 2 < value.length()) {
                int high = Character.digit(value.charAt(index + 1), 16);
                int low = Character.digit(value.charAt(index + 2), 16);
                if (high >= 0 && low >= 0) {
                    bytes[length++] = (byte) ((high << 4) + low);
                    index += 2;
                    continue;
                }
            }
            byte[] raw = String.valueOf(current).getBytes(StandardCharsets.UTF_8);
            System.arraycopy(raw, 0, bytes, length, raw.length);
            length += raw.length;
        }
        return new String(bytes, 0, length, StandardCharsets.UTF_8);
    }

    private static byte[] deriveSigningKey(String secret, String date, String region, String service) {
        byte[] dateKey = hmacSha256(("AWS4" + secret).getBytes(StandardCharsets.UTF_8), date);
        byte[] regionKey = hmacSha256(dateKey, region);
        byte[] serviceKey = hmacSha256(regionKey, service);
        return hmacSha256(serviceKey, "aws4_request");
    }

    private static String sha256Hex(String value) {
        try {
            return hexEncode(MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) {
            throw new IllegalStateException("Failed to compute SHA-256", e);
        }
    }
    private static byte[] hmacSha256(byte[] key, String data) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(key, "HmacSHA256"));
            return mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            throw new IllegalStateException("Failed to compute HMAC-SHA256", e);
        }
    }
    private static String hexEncode(byte[] bytes) {
        StringBuilder hex = new StringBuilder(bytes.length * 2);
        for (byte value : bytes) {
            hex.append(String.format("%02x", value));
        }
        return hex.toString();
    }

    private record QueryParameter(String name, String value) {
    }
}
