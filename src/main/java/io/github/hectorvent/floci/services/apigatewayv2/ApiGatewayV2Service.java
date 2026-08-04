package io.github.hectorvent.floci.services.apigatewayv2;

import com.fasterxml.jackson.core.type.TypeReference;
import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.core.common.AwsArnUtils;
import io.github.hectorvent.floci.core.common.AwsException;
import io.github.hectorvent.floci.core.common.RegionResolver;
import io.github.hectorvent.floci.core.common.ReservedTags;
import io.github.hectorvent.floci.core.storage.AccountAwareStorageBackend;
import io.github.hectorvent.floci.core.storage.StorageBackend;
import io.github.hectorvent.floci.core.storage.StorageFactory;
import io.github.hectorvent.floci.services.apigatewayv2.model.*;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Pattern;

@ApplicationScoped
public class ApiGatewayV2Service {

    private static final Logger LOG = Logger.getLogger(ApiGatewayV2Service.class);

    private final StorageBackend<String, Api> apiStore;
    private final StorageBackend<String, Route> routeStore;
    private final StorageBackend<String, Integration> integrationStore;
    private final StorageBackend<String, Authorizer> authorizerStore;
    private final StorageBackend<String, Deployment> deploymentStore;
    private final StorageBackend<String, Stage> stageStore;
    private final StorageBackend<String, RouteResponse> routeResponseStore;
    private final StorageBackend<String, IntegrationResponse> integrationResponseStore;
    private final StorageBackend<String, Model> modelStore;
    private final StorageBackend<String, VpcLink> vpcLinkStore;
    private final StorageBackend<String, DomainName> domainNameStore;
    private final StorageBackend<String, ApiMapping> apiMappingStore;
    private final RegionResolver regionResolver;

    public record ApiOwner(String accountId, String region) {}

    @Inject
    public ApiGatewayV2Service(StorageFactory storageFactory, EmulatorConfig config, RegionResolver regionResolver) {
        this.apiStore = storageFactory.create("apigatewayv2", "apigatewayv2-apis.json",
                new TypeReference<>() {});
        this.routeStore = storageFactory.create("apigatewayv2", "apigatewayv2-routes.json",
                new TypeReference<>() {});
        this.integrationStore = storageFactory.create("apigatewayv2", "apigatewayv2-integrations.json",
                new TypeReference<>() {});
        this.authorizerStore = storageFactory.create("apigatewayv2", "apigatewayv2-authorizers.json",
                new TypeReference<>() {});
        this.deploymentStore = storageFactory.create("apigatewayv2", "apigatewayv2-deployments.json",
                new TypeReference<>() {});
        this.stageStore = storageFactory.create("apigatewayv2", "apigatewayv2-stages.json",
                new TypeReference<>() {});
        this.routeResponseStore = storageFactory.create("apigatewayv2", "apigatewayv2-routeresponses.json",
                new TypeReference<>() {});
        this.integrationResponseStore = storageFactory.create("apigatewayv2", "apigatewayv2-integrationresponses.json",
                new TypeReference<>() {});
        this.modelStore = storageFactory.create("apigatewayv2", "apigatewayv2-models.json",
                new TypeReference<>() {});
        this.vpcLinkStore = storageFactory.create("apigatewayv2", "apigatewayv2-vpclinks.json",
                new TypeReference<>() {});
        this.domainNameStore = storageFactory.create("apigatewayv2", "apigatewayv2-domainnames.json",
                new TypeReference<>() {});
        this.apiMappingStore = storageFactory.create("apigatewayv2", "apigatewayv2-apimappings.json",
                new TypeReference<>() {});
        this.regionResolver = regionResolver;
    }

    // ──────────────────────────── API CRUD ────────────────────────────

    public Api createApi(String region, Map<String, Object> request) {
        String name = (String) request.get("name");
        String protocolType = (String) request.getOrDefault("protocolType", "HTTP");
        String routeSelectionExpression = (String) request.get("routeSelectionExpression");
        String description = (String) request.get("description");
        String apiKeySelectionExpression = (String) request.get("apiKeySelectionExpression");

        if ("WEBSOCKET".equals(protocolType) && (routeSelectionExpression == null || routeSelectionExpression.isBlank())) {
            throw new AwsException("BadRequestException",
                    "RouteSelectionExpression is required for WEBSOCKET protocol", 400);
        }

        // Apply AWS defaults
        if (apiKeySelectionExpression == null) {
            apiKeySelectionExpression = "$request.header.x-api-key";
        }
        if ("HTTP".equals(protocolType) && routeSelectionExpression == null) {
            routeSelectionExpression = "${request.method} ${request.path}";
        }

        @SuppressWarnings("unchecked")
        Map<String, String> tags = (Map<String, String>) request.get("tags");
        String overrideId = ReservedTags.extractOverrideApiId(tags);
        String apiId = overrideId != null ? overrideId : shortId(10);
        if (apiIdExists(apiId)) {
            throw new AwsException("ConflictException",
                    "API with id '" + apiId + "' already exists", 409);
        }

        Api api = new Api();
        api.setApiId(apiId);
        api.setName(name);
        api.setProtocolType(protocolType);
        api.setCreatedDate(System.currentTimeMillis());
        api.setRouteSelectionExpression(routeSelectionExpression);
        api.setDescription(description);
        api.setApiKeySelectionExpression(apiKeySelectionExpression);

        if ("WEBSOCKET".equals(protocolType)) {
            api.setApiEndpoint(String.format("wss://%s.execute-api.%s.amazonaws.com", api.getApiId(), region));
        } else {
            api.setApiEndpoint(String.format("https://%s.execute-api.%s.amazonaws.com", api.getApiId(), region));
        }

        if (tags != null) {
            api.setTags(ReservedTags.stripApiGatewayReservedTags(tags));
        }

        @SuppressWarnings("unchecked")
        Map<String, Object> corsConfig = (Map<String, Object>) request.get("corsConfiguration");
        if (corsConfig != null) {
            api.setCorsConfiguration(toCors(corsConfig));
        }

        apiStore.put(apiKey(region, api.getApiId()), api);
        LOG.infov("Created {0} API: {1} ({2}) in {3}", protocolType, api.getName(), api.getApiId(), region);
        return api;
    }

    public Api getApi(String region, String apiId) {
        return apiStore.get(apiKey(region, apiId))
                .orElseThrow(() -> new AwsException("NotFoundException", "Invalid API id specified", 404));
    }

    /** Resolves the account and region that own an API ID for unsigned data-plane requests. */
    public Optional<ApiOwner> findApiOwner(String apiId) {
        if (!(apiStore instanceof AccountAwareStorageBackend<Api> accountAware)) {
            return apiStore.keys().stream()
                    .filter(key -> key.endsWith("::" + apiId))
                    .map(key -> new ApiOwner(regionResolver.getAccountId(), regionFromApiKey(key)))
                    .findFirst();
        }

        List<AccountAwareStorageBackend.AccountEntry<Api>> matches = accountAware
                .scanAllAccountEntries(key -> key.endsWith("::" + apiId));
        if (matches.size() > 1) {
            throw new AwsException("ConflictException",
                    "API id '" + apiId + "' is ambiguous across accounts", 409);
        }
        return matches.stream()
                .findFirst()
                .map(entry -> new ApiOwner(entry.accountId(), regionFromApiKey(entry.key())));
    }

    private boolean apiIdExists(String apiId) {
        return findApiOwner(apiId).isPresent();
    }

    private String regionFromApiKey(String key) {
        int delimiter = key.indexOf("::");
        if (delimiter <= 0) {
            throw new IllegalStateException("Invalid API storage key: " + key);
        }
        return key.substring(0, delimiter);
    }

    public List<Api> getApis(String region) {
        String prefix = region + "::";
        return apiStore.scan(k -> k.startsWith(prefix));
    }

    public void deleteApi(String region, String apiId) {
        getApi(region, apiId);
        apiStore.delete(apiKey(region, apiId));
        // Cascade-delete every child resource keyed under region::apiId::*.
        // VpcLink is region-scoped (region::vpcLinkId, shared across APIs) and excluded.
        String prefix = region + "::" + apiId + "::";
        deleteByPrefix(routeStore, prefix);
        deleteByPrefix(integrationStore, prefix);
        deleteByPrefix(authorizerStore, prefix);
        deleteByPrefix(deploymentStore, prefix);
        deleteByPrefix(stageStore, prefix);
        deleteByPrefix(modelStore, prefix);
        deleteByPrefix(routeResponseStore, prefix);
        deleteByPrefix(integrationResponseStore, prefix);
        apiMappingStore.keys().stream()
                .filter(key -> key.startsWith(region + "::"))
                .filter(key -> apiMappingStore.get(key).map(mapping -> apiId.equals(mapping.getApiId())).orElse(false))
                .forEach(apiMappingStore::delete);
        LOG.infov("Deleted HTTP API: {0} in {1}", apiId, region);
    }

    private static void deleteByPrefix(StorageBackend<String, ?> store, String prefix) {
        store.keys().stream().filter(k -> k.startsWith(prefix)).forEach(store::delete);
    }

    public Api updateApi(String region, String apiId, Map<String, Object> request) {
        Api api = getApi(region, apiId);

        if (request.containsKey("name") && request.get("name") != null) {
            api.setName((String) request.get("name"));
        }
        if (request.containsKey("description") && request.get("description") != null) {
            api.setDescription((String) request.get("description"));
        }
        if (request.containsKey("routeSelectionExpression") && request.get("routeSelectionExpression") != null) {
            api.setRouteSelectionExpression((String) request.get("routeSelectionExpression"));
        }
        if (request.containsKey("apiKeySelectionExpression") && request.get("apiKeySelectionExpression") != null) {
            api.setApiKeySelectionExpression((String) request.get("apiKeySelectionExpression"));
        }
        if (request.containsKey("tags")) {
            @SuppressWarnings("unchecked")
            Map<String, String> tags = (Map<String, String>) request.get("tags");
            ReservedTags.rejectApiGatewayReservedTagsOnUpdate(tags);
            api.setTags(tags);
        }
        if (request.containsKey("corsConfiguration")) {
            @SuppressWarnings("unchecked")
            Map<String, Object> corsConfig = (Map<String, Object>) request.get("corsConfiguration");
            api.setCorsConfiguration(corsConfig == null ? null : toCors(corsConfig));
        }

        apiStore.put(apiKey(region, apiId), api);
        return api;
    }

    private static Api.Cors toCors(Map<String, Object> m) {
        @SuppressWarnings("unchecked")
        List<String> allowOrigins = (List<String>) m.get("allowOrigins");
        @SuppressWarnings("unchecked")
        List<String> allowMethods = (List<String>) m.get("allowMethods");
        @SuppressWarnings("unchecked")
        List<String> allowHeaders = (List<String>) m.get("allowHeaders");
        @SuppressWarnings("unchecked")
        List<String> exposeHeaders = (List<String>) m.get("exposeHeaders");
        Integer maxAge = m.get("maxAge") == null ? null : ((Number) m.get("maxAge")).intValue();
        Boolean allowCredentials = m.get("allowCredentials") == null
                ? null
                : Boolean.parseBoolean(String.valueOf(m.get("allowCredentials")));
        return new Api.Cors(allowOrigins, allowMethods, allowHeaders, exposeHeaders, maxAge, allowCredentials);
    }

    // ──────────────────────────── Authorizer CRUD ────────────────────────────

    public Authorizer createAuthorizer(String region, String apiId, Map<String, Object> request) {
        getApi(region, apiId);
        Authorizer auth = new Authorizer();
        auth.setAuthorizerId(shortId(8));
        auth.setName((String) request.get("name"));
        auth.setAuthorizerType((String) request.get("authorizerType"));

        Object identitySourceRaw = request.get("identitySource");
        if (identitySourceRaw instanceof String s) {
            auth.setIdentitySource(List.of(s));
        } else if (identitySourceRaw instanceof List<?>) {
            @SuppressWarnings("unchecked")
            List<String> identitySource = (List<String>) identitySourceRaw;
            auth.setIdentitySource(identitySource);
        }

        @SuppressWarnings("unchecked")
        Map<String, Object> jwtConfig = (Map<String, Object>) request.get("jwtConfiguration");
        if (jwtConfig != null) {
            @SuppressWarnings("unchecked")
            List<String> audience = (List<String>) jwtConfig.get("audience");
            String issuer = (String) jwtConfig.get("issuer");
            auth.setJwtConfiguration(new Authorizer.JwtConfiguration(audience, issuer));
        }

        auth.setAuthorizerUri((String) request.get("authorizerUri"));
        auth.setAuthorizerPayloadFormatVersion((String) request.get("authorizerPayloadFormatVersion"));
        if (request.get("authorizerResultTtlInSeconds") != null) {
            auth.setAuthorizerResultTtlInSeconds(((Number) request.get("authorizerResultTtlInSeconds")).intValue());
        }
        if (request.get("enableSimpleResponses") != null) {
            auth.setEnableSimpleResponses(Boolean.parseBoolean(String.valueOf(request.get("enableSimpleResponses"))));
        }

        authorizerStore.put(authorizerKey(region, apiId, auth.getAuthorizerId()), auth);
        LOG.infov("Created authorizer: {0} ({1}) for API {2}", auth.getName(), auth.getAuthorizerId(), apiId);
        return auth;
    }

    public Authorizer getAuthorizer(String region, String apiId, String authorizerId) {
        return authorizerStore.get(authorizerKey(region, apiId, authorizerId))
                .orElseThrow(() -> new AwsException("NotFoundException", "Authorizer not found", 404));
    }

    public List<Authorizer> getAuthorizers(String region, String apiId) {
        getApi(region, apiId);
        String prefix = region + "::" + apiId + "::";
        return authorizerStore.scan(k -> k.startsWith(prefix));
    }

    public void deleteAuthorizer(String region, String apiId, String authorizerId) {
        getAuthorizer(region, apiId, authorizerId);
        authorizerStore.delete(authorizerKey(region, apiId, authorizerId));
    }

    public Authorizer updateAuthorizer(String region, String apiId, String authorizerId,
                                       Map<String, Object> request) {
        Authorizer auth = getAuthorizer(region, apiId, authorizerId);

        if (request.containsKey("name") && request.get("name") != null) {
            auth.setName((String) request.get("name"));
        }
        if (request.containsKey("authorizerType") && request.get("authorizerType") != null) {
            auth.setAuthorizerType((String) request.get("authorizerType"));
        }
        if (request.containsKey("identitySource") && request.get("identitySource") != null) {
            Object identitySourceRaw = request.get("identitySource");
            if (identitySourceRaw instanceof String s) {
                auth.setIdentitySource(List.of(s));
            } else if (identitySourceRaw instanceof List<?>) {
                @SuppressWarnings("unchecked")
                List<String> identitySource = (List<String>) identitySourceRaw;
                auth.setIdentitySource(identitySource);
            }
        }
        if (request.containsKey("jwtConfiguration") && request.get("jwtConfiguration") != null) {
            @SuppressWarnings("unchecked")
            Map<String, Object> jwtConfig = (Map<String, Object>) request.get("jwtConfiguration");
            @SuppressWarnings("unchecked")
            List<String> audience = (List<String>) jwtConfig.get("audience");
            String issuer = (String) jwtConfig.get("issuer");
            auth.setJwtConfiguration(new Authorizer.JwtConfiguration(audience, issuer));
        }
        if (request.containsKey("authorizerUri") && request.get("authorizerUri") != null) {
            auth.setAuthorizerUri((String) request.get("authorizerUri"));
        }
        if (request.containsKey("authorizerPayloadFormatVersion") && request.get("authorizerPayloadFormatVersion") != null) {
            auth.setAuthorizerPayloadFormatVersion((String) request.get("authorizerPayloadFormatVersion"));
        }
        if (request.containsKey("authorizerResultTtlInSeconds") && request.get("authorizerResultTtlInSeconds") != null) {
            auth.setAuthorizerResultTtlInSeconds(((Number) request.get("authorizerResultTtlInSeconds")).intValue());
        }
        if (request.containsKey("enableSimpleResponses") && request.get("enableSimpleResponses") != null) {
            auth.setEnableSimpleResponses(Boolean.parseBoolean(String.valueOf(request.get("enableSimpleResponses"))));
        }

        authorizerStore.put(authorizerKey(region, apiId, authorizerId), auth);
        return auth;
    }

    // ──────────────────────────── Route CRUD ────────────────────────────

    public Route createRoute(String region, String apiId, Map<String, Object> request) {
        getApi(region, apiId);
        Route route = new Route();
        route.setRouteId(shortId(8));
        route.setRouteKey((String) request.get("routeKey"));
        route.setAuthorizationType((String) request.getOrDefault("authorizationType", "NONE"));
        route.setAuthorizerId((String) request.get("authorizerId"));
        route.setTarget((String) request.get("target"));
        route.setRouteResponseSelectionExpression((String) request.get("routeResponseSelectionExpression"));

        routeStore.put(routeKey(region, apiId, route.getRouteId()), route);
        return route;
    }

    public Route getRoute(String region, String apiId, String routeId) {
        return routeStore.get(routeKey(region, apiId, routeId))
                .orElseThrow(() -> new AwsException("NotFoundException", "Route not found", 404));
    }

    public List<Route> getRoutes(String region, String apiId) {
        getApi(region, apiId);
        String prefix = region + "::" + apiId + "::";
        return routeStore.scan(k -> k.startsWith(prefix));
    }

    public void deleteRoute(String region, String apiId, String routeId) {
        getRoute(region, apiId, routeId);
        routeStore.delete(routeKey(region, apiId, routeId));
    }

    public Route updateRoute(String region, String apiId, String routeId, Map<String, Object> request) {
        Route route = getRoute(region, apiId, routeId);

        if (request.containsKey("routeKey") && request.get("routeKey") != null) {
            route.setRouteKey((String) request.get("routeKey"));
        }
        if (request.containsKey("authorizationType") && request.get("authorizationType") != null) {
            route.setAuthorizationType((String) request.get("authorizationType"));
        }
        if (request.containsKey("authorizerId") && request.get("authorizerId") != null) {
            route.setAuthorizerId((String) request.get("authorizerId"));
        }
        if (request.containsKey("target") && request.get("target") != null) {
            route.setTarget((String) request.get("target"));
        }
        if (request.containsKey("routeResponseSelectionExpression") && request.get("routeResponseSelectionExpression") != null) {
            route.setRouteResponseSelectionExpression((String) request.get("routeResponseSelectionExpression"));
        }

        routeStore.put(routeKey(region, apiId, routeId), route);
        return route;
    }

    /**
     * Finds the best matching route for the given HTTP method and path.
     * Priority: exact match > path-template match > $default.
     */
    public Route findMatchingRoute(String region, String apiId, String httpMethod, String path) {
        List<Route> routes = getRoutes(region, apiId);
        String candidate = httpMethod.toUpperCase() + " " + path;

        // 1. Exact match
        for (Route r : routes) {
            if (candidate.equals(r.getRouteKey())) return r;
        }

        // 2. Path-template match (e.g. "GET /users/{id}")
        for (Route r : routes) {
            if (r.getRouteKey() == null || r.getRouteKey().equals("$default")) continue;
            if (routeKeyMatchesPath(r.getRouteKey(), httpMethod, path)) return r;
        }

        // 3. $default catch-all
        for (Route r : routes) {
            if ("$default".equals(r.getRouteKey())) return r;
        }

        return null;
    }

    /**
     * Finds a route by its exact routeKey (e.g. "$connect", "$disconnect", "$default").
     * Returns null if no route with the given key exists on the API.
     */
    public Route findRouteByKey(String region, String apiId, String routeKey) {
        List<Route> routes = getRoutes(region, apiId);
        for (Route r : routes) {
            if (routeKey.equals(r.getRouteKey())) {
                return r;
            }
        }
        return null;
    }

    private boolean routeKeyMatchesPath(String routeKey, String httpMethod, String path) {
        int space = routeKey.indexOf(' ');
        if (space < 0) return false;
        String method = routeKey.substring(0, space);
        String pattern = routeKey.substring(space + 1);
        // ANY is the AWS wildcard method — matches any inbound HTTP method.
        if (!"ANY".equalsIgnoreCase(method) && !method.equalsIgnoreCase(httpMethod)) return false;

        // Build regex from path template: {proxy+} -> .+, {param} -> [^/]+
        // Quote literal segments to avoid regex injection from path patterns
        StringBuilder regex = new StringBuilder("^");
        java.util.regex.Matcher m = java.util.regex.Pattern.compile("\\{([^}]*)}").matcher(pattern);
        int last = 0;
        while (m.find()) {
            regex.append(Pattern.quote(pattern.substring(last, m.start())));
            regex.append(m.group(1).endsWith("+") ? ".*" : "[^/]+");
            last = m.end();
        }
        regex.append(Pattern.quote(pattern.substring(last)));
        regex.append("$");
        return path.matches(regex.toString());
    }

    // ──────────────────────────── Integration CRUD ────────────────────────────

    public Integration createIntegration(String region, String apiId, Map<String, Object> request) {
        getApi(region, apiId);
        Integration integration = new Integration();
        integration.setIntegrationId(shortId(8));
        integration.setIntegrationType((String) request.get("integrationType"));
        integration.setIntegrationUri((String) request.get("integrationUri"));
        integration.setConnectionType((String) request.get("connectionType"));
        integration.setPayloadFormatVersion((String) request.getOrDefault("payloadFormatVersion", "2.0"));
        integration.setIntegrationMethod((String) request.get("integrationMethod"));
        integration.setTemplateSelectionExpression((String) request.get("templateSelectionExpression"));

        if (request.get("timeoutInMillis") != null) {
            integration.setTimeoutInMillis(((Number) request.get("timeoutInMillis")).intValue());
        }

        @SuppressWarnings("unchecked")
        Map<String, String> requestTemplates = (Map<String, String>) request.get("requestTemplates");
        integration.setRequestTemplates(requestTemplates);

        @SuppressWarnings("unchecked")
        Map<String, String> responseTemplates = (Map<String, String>) request.get("responseTemplates");
        integration.setResponseTemplates(responseTemplates);

        @SuppressWarnings("unchecked")
        Map<String, String> requestParameters = (Map<String, String>) request.get("requestParameters");
        integration.setRequestParameters(requestParameters);

        integration.setConnectionId((String) request.get("connectionId"));

        integrationStore.put(integrationKey(region, apiId, integration.getIntegrationId()), integration);
        return integration;
    }

    public Integration getIntegration(String region, String apiId, String integrationId) {
        return integrationStore.get(integrationKey(region, apiId, integrationId))
                .orElseThrow(() -> new AwsException("NotFoundException", "Integration not found", 404));
    }

    public List<Integration> getIntegrations(String region, String apiId) {
        getApi(region, apiId);
        String prefix = region + "::" + apiId + "::";
        return integrationStore.scan(k -> k.startsWith(prefix));
    }

    public void deleteIntegration(String region, String apiId, String integrationId) {
        getIntegration(region, apiId, integrationId);
        integrationStore.delete(integrationKey(region, apiId, integrationId));
    }

    public Integration updateIntegration(String region, String apiId, String integrationId,
                                         Map<String, Object> request) {
        Integration integration = getIntegration(region, apiId, integrationId);

        if (request.containsKey("integrationType") && request.get("integrationType") != null) {
            integration.setIntegrationType((String) request.get("integrationType"));
        }
        if (request.containsKey("integrationUri") && request.get("integrationUri") != null) {
            integration.setIntegrationUri((String) request.get("integrationUri"));
        }
        if (request.containsKey("connectionType") && request.get("connectionType") != null) {
            integration.setConnectionType((String) request.get("connectionType"));
        }
        if (request.containsKey("payloadFormatVersion") && request.get("payloadFormatVersion") != null) {
            integration.setPayloadFormatVersion((String) request.get("payloadFormatVersion"));
        }
        if (request.containsKey("integrationMethod") && request.get("integrationMethod") != null) {
            integration.setIntegrationMethod((String) request.get("integrationMethod"));
        }
        if (request.containsKey("templateSelectionExpression") && request.get("templateSelectionExpression") != null) {
            integration.setTemplateSelectionExpression((String) request.get("templateSelectionExpression"));
        }
        if (request.containsKey("timeoutInMillis") && request.get("timeoutInMillis") != null) {
            integration.setTimeoutInMillis(((Number) request.get("timeoutInMillis")).intValue());
        }
        if (request.containsKey("requestTemplates") && request.get("requestTemplates") != null) {
            @SuppressWarnings("unchecked")
            Map<String, String> requestTemplates = (Map<String, String>) request.get("requestTemplates");
            integration.setRequestTemplates(requestTemplates);
        }
        if (request.containsKey("responseTemplates") && request.get("responseTemplates") != null) {
            @SuppressWarnings("unchecked")
            Map<String, String> responseTemplates = (Map<String, String>) request.get("responseTemplates");
            integration.setResponseTemplates(responseTemplates);
        }
        if (request.containsKey("requestParameters") && request.get("requestParameters") != null) {
            @SuppressWarnings("unchecked")
            Map<String, String> requestParameters = (Map<String, String>) request.get("requestParameters");
            integration.setRequestParameters(requestParameters);
        }
        if (request.containsKey("connectionId") && request.get("connectionId") != null) {
            integration.setConnectionId((String) request.get("connectionId"));
        }

        integrationStore.put(integrationKey(region, apiId, integrationId), integration);
        return integration;
    }

    // ──────────────────────────── Stage CRUD ────────────────────────────

    public Stage createStage(String region, String apiId, Map<String, Object> request) {
        getApi(region, apiId);
        Stage stage = new Stage();
        stage.setStageName((String) request.getOrDefault("stageName", "$default"));
        stage.setDeploymentId((String) request.get("deploymentId"));
        stage.setAutoDeploy(Boolean.parseBoolean(String.valueOf(request.getOrDefault("autoDeploy", "false"))));
        stage.setCreatedDate(System.currentTimeMillis());
        stage.setLastUpdatedDate(System.currentTimeMillis());

        @SuppressWarnings("unchecked")
        Map<String, String> stageVariables = (Map<String, String>) request.get("stageVariables");
        stage.setStageVariables(stageVariables);

        @SuppressWarnings("unchecked")
        Map<String, String> tags = (Map<String, String>) request.get("tags");
        if (tags != null) {
            stage.setTags(ReservedTags.stripApiGatewayReservedTags(tags));
        }

        stageStore.put(stageKey(region, apiId, stage.getStageName()), stage);
        LOG.infov("Created stage: {0} for API {1}", stage.getStageName(), apiId);
        return stage;
    }

    public Stage getStage(String region, String apiId, String stageName) {
        return stageStore.get(stageKey(region, apiId, stageName))
                .orElseThrow(() -> new AwsException("NotFoundException", "Stage not found", 404));
    }

    public List<Stage> getStages(String region, String apiId) {
        getApi(region, apiId);
        String prefix = region + "::" + apiId + "::";
        return stageStore.scan(k -> k.startsWith(prefix));
    }

    public void deleteStage(String region, String apiId, String stageName) {
        getStage(region, apiId, stageName);
        stageStore.delete(stageKey(region, apiId, stageName));
    }

    public Stage updateStage(String region, String apiId, String stageName,
                             Map<String, Object> request) {
        Stage stage = getStage(region, apiId, stageName);

        if (request.containsKey("deploymentId") && request.get("deploymentId") != null) {
            stage.setDeploymentId((String) request.get("deploymentId"));
        }
        if (request.containsKey("autoDeploy") && request.get("autoDeploy") != null) {
            stage.setAutoDeploy(Boolean.parseBoolean(String.valueOf(request.get("autoDeploy"))));
        }
        if (request.containsKey("stageVariables") && request.get("stageVariables") != null) {
            @SuppressWarnings("unchecked")
            Map<String, String> stageVariables = (Map<String, String>) request.get("stageVariables");
            stage.setStageVariables(stageVariables);
        }

        stage.setLastUpdatedDate(System.currentTimeMillis());
        stageStore.put(stageKey(region, apiId, stageName), stage);
        return stage;
    }

    // ──────────────────────────── Deployment CRUD ────────────────────────────

    public Deployment createDeployment(String region, String apiId, Map<String, Object> request) {
        getApi(region, apiId);

        // Validate stage exists before creating deployment to avoid orphans
        String stageName = (String) request.get("stageName");
        Stage stage = null;
        if (stageName != null && !stageName.isBlank()) {
            stage = stageStore.get(stageKey(region, apiId, stageName))
                    .orElseThrow(() -> new AwsException("NotFoundException",
                            "Stage " + stageName + " not found", 404));
        }

        Deployment deployment = new Deployment();
        deployment.setDeploymentId(shortId(8));
        deployment.setDeploymentStatus("DEPLOYED");
        deployment.setDescription((String) request.get("description"));
        deployment.setCreatedDate(System.currentTimeMillis());

        deploymentStore.put(deploymentKey(region, apiId, deployment.getDeploymentId()), deployment);

        if (stage != null) {
            stage.setDeploymentId(deployment.getDeploymentId());
            stage.setLastUpdatedDate(System.currentTimeMillis());
            stageStore.put(stageKey(region, apiId, stageName), stage);
        }

        return deployment;
    }

    public Deployment getDeployment(String region, String apiId, String deploymentId) {
        return deploymentStore.get(deploymentKey(region, apiId, deploymentId))
                .orElseThrow(() -> new AwsException("NotFoundException", "Deployment not found", 404));
    }

    public List<Deployment> getDeployments(String region, String apiId) {
        getApi(region, apiId);
        String prefix = region + "::" + apiId + "::";
        return deploymentStore.scan(k -> k.startsWith(prefix));
    }

    public void deleteDeployment(String region, String apiId, String deploymentId) {
        getDeployment(region, apiId, deploymentId);
        deploymentStore.delete(deploymentKey(region, apiId, deploymentId));
    }

    public Deployment updateDeployment(String region, String apiId, String deploymentId,
                                       Map<String, Object> request) {
        Deployment deployment = getDeployment(region, apiId, deploymentId);

        if (request.containsKey("description") && request.get("description") != null) {
            deployment.setDescription((String) request.get("description"));
        }

        deploymentStore.put(deploymentKey(region, apiId, deploymentId), deployment);
        return deployment;
    }

    // ──────────────────────────── Route Response CRUD ────────────────────────────

    public RouteResponse createRouteResponse(String region, String apiId, String routeId, Map<String, Object> request) {
        getApi(region, apiId);
        getRoute(region, apiId, routeId);

        RouteResponse rr = new RouteResponse();
        rr.setRouteResponseId(shortId(8));
        rr.setRouteId(routeId);
        rr.setRouteResponseKey((String) request.get("routeResponseKey"));
        rr.setModelSelectionExpression((String) request.get("modelSelectionExpression"));

        @SuppressWarnings("unchecked")
        Map<String, String> responseModels = (Map<String, String>) request.get("responseModels");
        rr.setResponseModels(responseModels);

        @SuppressWarnings("unchecked")
        Map<String, String> responseParameters = (Map<String, String>) request.get("responseParameters");
        rr.setResponseParameters(responseParameters);

        routeResponseStore.put(routeResponseKey(region, apiId, routeId, rr.getRouteResponseId()), rr);
        return rr;
    }

    public RouteResponse getRouteResponse(String region, String apiId, String routeId, String routeResponseId) {
        return routeResponseStore.get(routeResponseKey(region, apiId, routeId, routeResponseId))
                .orElseThrow(() -> new AwsException("NotFoundException", "Route response not found", 404));
    }

    public List<RouteResponse> getRouteResponses(String region, String apiId, String routeId) {
        getApi(region, apiId);
        String prefix = region + "::" + apiId + "::" + routeId + "::";
        return routeResponseStore.scan(k -> k.startsWith(prefix));
    }

    public RouteResponse updateRouteResponse(String region, String apiId, String routeId, String routeResponseId, Map<String, Object> request) {
        RouteResponse rr = getRouteResponse(region, apiId, routeId, routeResponseId);

        if (request.containsKey("routeResponseKey") && request.get("routeResponseKey") != null) {
            rr.setRouteResponseKey((String) request.get("routeResponseKey"));
        }
        if (request.containsKey("modelSelectionExpression") && request.get("modelSelectionExpression") != null) {
            rr.setModelSelectionExpression((String) request.get("modelSelectionExpression"));
        }
        if (request.containsKey("responseModels") && request.get("responseModels") != null) {
            @SuppressWarnings("unchecked")
            Map<String, String> responseModels = (Map<String, String>) request.get("responseModels");
            rr.setResponseModels(responseModels);
        }
        if (request.containsKey("responseParameters") && request.get("responseParameters") != null) {
            @SuppressWarnings("unchecked")
            Map<String, String> responseParameters = (Map<String, String>) request.get("responseParameters");
            rr.setResponseParameters(responseParameters);
        }

        routeResponseStore.put(routeResponseKey(region, apiId, routeId, routeResponseId), rr);
        return rr;
    }

    public void deleteRouteResponse(String region, String apiId, String routeId, String routeResponseId) {
        getRouteResponse(region, apiId, routeId, routeResponseId);
        routeResponseStore.delete(routeResponseKey(region, apiId, routeId, routeResponseId));
    }

    // ──────────────────────────── Integration Response CRUD ────────────────────────────

    public IntegrationResponse createIntegrationResponse(String region, String apiId, String integrationId, Map<String, Object> request) {
        getApi(region, apiId);
        getIntegration(region, apiId, integrationId);

        IntegrationResponse ir = new IntegrationResponse();
        ir.setIntegrationResponseId(shortId(8));
        ir.setIntegrationId(integrationId);
        ir.setIntegrationResponseKey((String) request.get("integrationResponseKey"));
        ir.setContentHandlingStrategy((String) request.get("contentHandlingStrategy"));
        ir.setTemplateSelectionExpression((String) request.get("templateSelectionExpression"));

        @SuppressWarnings("unchecked")
        Map<String, String> responseTemplates = (Map<String, String>) request.get("responseTemplates");
        ir.setResponseTemplates(responseTemplates);

        @SuppressWarnings("unchecked")
        Map<String, String> responseParameters = (Map<String, String>) request.get("responseParameters");
        ir.setResponseParameters(responseParameters);

        integrationResponseStore.put(integrationResponseKey(region, apiId, integrationId, ir.getIntegrationResponseId()), ir);
        return ir;
    }

    public IntegrationResponse getIntegrationResponse(String region, String apiId, String integrationId, String integrationResponseId) {
        return integrationResponseStore.get(integrationResponseKey(region, apiId, integrationId, integrationResponseId))
                .orElseThrow(() -> new AwsException("NotFoundException", "Integration response not found", 404));
    }

    public List<IntegrationResponse> getIntegrationResponses(String region, String apiId, String integrationId) {
        getApi(region, apiId);
        String prefix = region + "::" + apiId + "::" + integrationId + "::";
        return integrationResponseStore.scan(k -> k.startsWith(prefix));
    }

    public IntegrationResponse updateIntegrationResponse(String region, String apiId, String integrationId, String integrationResponseId, Map<String, Object> request) {
        IntegrationResponse ir = getIntegrationResponse(region, apiId, integrationId, integrationResponseId);

        if (request.containsKey("integrationResponseKey") && request.get("integrationResponseKey") != null) {
            ir.setIntegrationResponseKey((String) request.get("integrationResponseKey"));
        }
        if (request.containsKey("contentHandlingStrategy") && request.get("contentHandlingStrategy") != null) {
            ir.setContentHandlingStrategy((String) request.get("contentHandlingStrategy"));
        }
        if (request.containsKey("templateSelectionExpression") && request.get("templateSelectionExpression") != null) {
            ir.setTemplateSelectionExpression((String) request.get("templateSelectionExpression"));
        }
        if (request.containsKey("responseTemplates") && request.get("responseTemplates") != null) {
            @SuppressWarnings("unchecked")
            Map<String, String> responseTemplates = (Map<String, String>) request.get("responseTemplates");
            ir.setResponseTemplates(responseTemplates);
        }
        if (request.containsKey("responseParameters") && request.get("responseParameters") != null) {
            @SuppressWarnings("unchecked")
            Map<String, String> responseParameters = (Map<String, String>) request.get("responseParameters");
            ir.setResponseParameters(responseParameters);
        }

        integrationResponseStore.put(integrationResponseKey(region, apiId, integrationId, integrationResponseId), ir);
        return ir;
    }

    public void deleteIntegrationResponse(String region, String apiId, String integrationId, String integrationResponseId) {
        getIntegrationResponse(region, apiId, integrationId, integrationResponseId);
        integrationResponseStore.delete(integrationResponseKey(region, apiId, integrationId, integrationResponseId));
    }

    // ──────────────────────────── Model CRUD ────────────────────────────

    public Model createModel(String region, String apiId, Map<String, Object> request) {
        getApi(region, apiId);
        Model model = new Model();
        model.setModelId(shortId(10));
        model.setName((String) request.get("name"));
        model.setSchema((String) request.get("schema"));
        model.setDescription((String) request.get("description"));
        model.setContentType((String) request.get("contentType"));
        modelStore.put(modelKey(region, apiId, model.getModelId()), model);
        return model;
    }

    public Model getModel(String region, String apiId, String modelId) {
        return modelStore.get(modelKey(region, apiId, modelId))
                .orElseThrow(() -> new AwsException("NotFoundException", "Model not found", 404));
    }

    public List<Model> getModels(String region, String apiId) {
        getApi(region, apiId);
        String prefix = region + "::" + apiId + "::";
        return modelStore.scan(k -> k.startsWith(prefix));
    }

    public Model updateModel(String region, String apiId, String modelId, Map<String, Object> request) {
        Model model = getModel(region, apiId, modelId);

        if (request.containsKey("name") && request.get("name") != null) {
            model.setName((String) request.get("name"));
        }
        if (request.containsKey("schema") && request.get("schema") != null) {
            model.setSchema((String) request.get("schema"));
        }
        if (request.containsKey("description") && request.get("description") != null) {
            model.setDescription((String) request.get("description"));
        }
        if (request.containsKey("contentType") && request.get("contentType") != null) {
            model.setContentType((String) request.get("contentType"));
        }

        modelStore.put(modelKey(region, apiId, modelId), model);
        return model;
    }

    public void deleteModel(String region, String apiId, String modelId) {
        getModel(region, apiId, modelId);
        modelStore.delete(modelKey(region, apiId, modelId));
    }

    // ──────────────────────────── VPC Link CRUD ────────────────────────────

    public VpcLink createVpcLink(String region, Map<String, Object> request) {
        VpcLink link = new VpcLink();
        link.setVpcLinkId(shortId(10));
        link.setName((String) request.get("name"));

        @SuppressWarnings("unchecked")
        List<String> subnetIds = (List<String>) request.get("subnetIds");
        link.setSubnetIds(subnetIds);

        @SuppressWarnings("unchecked")
        List<String> securityGroupIds = (List<String>) request.get("securityGroupIds");
        link.setSecurityGroupIds(securityGroupIds);

        // Floci has no real VPC — provision the link as AVAILABLE immediately.
        link.setVpcLinkStatus("AVAILABLE");
        link.setCreatedDate(System.currentTimeMillis());

        @SuppressWarnings("unchecked")
        Map<String, String> tags = (Map<String, String>) request.get("tags");
        if (tags != null) {
            link.setTags(tags);
        }

        vpcLinkStore.put(vpcLinkKey(region, link.getVpcLinkId()), link);
        LOG.infov("Created VPC Link: {0} ({1}) in {2}", link.getName(), link.getVpcLinkId(), region);
        return link;
    }

    public VpcLink getVpcLink(String region, String vpcLinkId) {
        return vpcLinkStore.get(vpcLinkKey(region, vpcLinkId))
                .orElseThrow(() -> new AwsException("NotFoundException", "VpcLink not found", 404));
    }

    public List<VpcLink> getVpcLinks(String region) {
        String prefix = region + "::";
        return vpcLinkStore.scan(k -> k.startsWith(prefix));
    }

    public void deleteVpcLink(String region, String vpcLinkId) {
        getVpcLink(region, vpcLinkId);
        vpcLinkStore.delete(vpcLinkKey(region, vpcLinkId));
    }

    // ──────────────────────────── Custom Domains & API Mappings ────────────────────────────

    public DomainName createDomainName(String region, Map<String, Object> request) {
        String name = (String) request.get("domainName");
        if (name == null || name.isBlank()) {
            throw new AwsException("BadRequestException", "DomainName must not be blank", 400);
        }
        String key = domainNameKey(region, name);
        if (domainNameStore.get(key).isPresent()) {
            throw new AwsException("ConflictException", "Domain name already exists", 409);
        }

        DomainName domain = new DomainName();
        domain.setDomainName(name);
        domain.setDomainNameArn("arn:aws:apigateway:" + region + "::/domainnames/" + name);
        domain.setDomainNameConfigurations(domainConfigurations(name, request.get("domainNameConfigurations")));
        domain.setMutualTlsAuthentication(objectMap(request.get("mutualTlsAuthentication")));
        domain.setRoutingMode((String) request.getOrDefault("routingMode", "API_MAPPING_ONLY"));
        domain.setApiMappingSelectionExpression("$request.basepath");
        domain.setTags(stringMap(request.get("tags")));
        domainNameStore.put(key, domain);
        return domain;
    }

    public DomainName getDomainName(String region, String name) {
        return domainNameStore.get(domainNameKey(region, name))
                .orElseThrow(() -> new AwsException("NotFoundException", "Domain name not found", 404));
    }

    public List<DomainName> getDomainNames(String region) {
        return domainNameStore.scan(key -> key.startsWith(region + "::"));
    }

    public DomainName updateDomainName(String region, String name, Map<String, Object> request) {
        DomainName domain = getDomainName(region, name);
        if (request.containsKey("domainNameConfigurations") && request.get("domainNameConfigurations") != null) {
            domain.setDomainNameConfigurations(domainConfigurations(name, request.get("domainNameConfigurations")));
        }
        if (request.containsKey("mutualTlsAuthentication")) {
            domain.setMutualTlsAuthentication(objectMap(request.get("mutualTlsAuthentication")));
        }
        if (request.containsKey("routingMode") && request.get("routingMode") != null) {
            domain.setRoutingMode((String) request.get("routingMode"));
        }
        domainNameStore.put(domainNameKey(region, name), domain);
        return domain;
    }

    public void deleteDomainName(String region, String name) {
        getDomainName(region, name);
        domainNameStore.delete(domainNameKey(region, name));
        String prefix = region + "::" + name + "::";
        deleteByPrefix(apiMappingStore, prefix);
    }

    public ApiMapping createApiMapping(String region, String domainName, Map<String, Object> request) {
        getDomainName(region, domainName);
        String apiId = (String) request.get("apiId");
        if (apiId == null || apiId.isBlank()) {
            throw new AwsException("BadRequestException", "ApiId must not be blank", 400);
        }
        getApi(region, apiId);
        String mappingKey = (String) request.get("apiMappingKey");
        String normalizedMappingKey = mappingKey == null ? "" : mappingKey;
        boolean duplicate = getApiMappings(region, domainName).stream()
                .anyMatch(mapping -> normalizedMappingKey.equals(
                        mapping.getApiMappingKey() == null ? "" : mapping.getApiMappingKey()));
        if (duplicate) {
            throw new AwsException("ConflictException", "API mapping key already exists", 409);
        }

        ApiMapping mapping = new ApiMapping();
        mapping.setApiMappingId(shortId(8));
        mapping.setApiId(apiId);
        mapping.setApiMappingKey(mappingKey);
        mapping.setStage((String) request.get("stage"));
        apiMappingStore.put(apiMappingKey(region, domainName, mapping.getApiMappingId()), mapping);
        return mapping;
    }

    public ApiMapping getApiMapping(String region, String domainName, String mappingId) {
        getDomainName(region, domainName);
        return apiMappingStore.get(apiMappingKey(region, domainName, mappingId))
                .orElseThrow(() -> new AwsException("NotFoundException", "API mapping not found", 404));
    }

    public List<ApiMapping> getApiMappings(String region, String domainName) {
        getDomainName(region, domainName);
        return apiMappingStore.scan(key -> key.startsWith(region + "::" + domainName + "::"));
    }

    public ApiMapping updateApiMapping(String region, String domainName, String mappingId, Map<String, Object> request) {
        ApiMapping mapping = getApiMapping(region, domainName, mappingId);
        if (request.containsKey("apiId") && request.get("apiId") != null) {
            String apiId = (String) request.get("apiId");
            getApi(region, apiId);
            mapping.setApiId(apiId);
        }
        if (request.containsKey("apiMappingKey")) {
            String mappingKey = (String) request.get("apiMappingKey");
            String normalizedMappingKey = mappingKey == null ? "" : mappingKey;
            boolean duplicate = getApiMappings(region, domainName).stream()
                    .filter(candidate -> !mappingId.equals(candidate.getApiMappingId()))
                    .anyMatch(candidate -> normalizedMappingKey.equals(
                            candidate.getApiMappingKey() == null ? "" : candidate.getApiMappingKey()));
            if (duplicate) {
                throw new AwsException("ConflictException", "API mapping key already exists", 409);
            }
            mapping.setApiMappingKey(mappingKey);
        }
        if (request.containsKey("stage")) {
            mapping.setStage((String) request.get("stage"));
        }
        apiMappingStore.put(apiMappingKey(region, domainName, mappingId), mapping);
        return mapping;
    }

    public void deleteApiMapping(String region, String domainName, String mappingId) {
        getApiMapping(region, domainName, mappingId);
        apiMappingStore.delete(apiMappingKey(region, domainName, mappingId));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> objectMap(Object value) {
        return value instanceof Map<?, ?> map ? new HashMap<>((Map<String, Object>) map) : null;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, String> stringMap(Object value) {
        return value instanceof Map<?, ?> map ? new HashMap<>((Map<String, String>) map) : null;
    }

    private static List<Map<String, Object>> domainConfigurations(String name, Object value) {
        if (!(value instanceof List<?> configurations)) {
            return new ArrayList<>();
        }
        List<Map<String, Object>> result = new ArrayList<>();
        for (Object configuration : configurations) {
            if (!(configuration instanceof Map<?, ?> map)) {
                throw new AwsException("BadRequestException", "DomainNameConfigurations must contain objects", 400);
            }
            Map<String, Object> normalized = new HashMap<>();
            map.forEach((key, entryValue) -> normalized.put(String.valueOf(key), entryValue));
            if ("REGIONAL".equals(normalized.get("endpointType"))) {
                normalized.putIfAbsent("apiGatewayDomainName", name + ".regional.local");
                normalized.putIfAbsent("hostedZoneId", "Z2FDTNDATAQYL2");
            }
            result.add(normalized);
        }
        return result;
    }

    // ──────────────────────────── Standalone Tagging ────────────────────────────

    private sealed interface TaggableResource permits ApiResource, StageResource, DomainNameResource {
        String region();
    }

    private record ApiResource(String region, String apiId) implements TaggableResource {}

    private record StageResource(String region, String apiId, String stageName) implements TaggableResource {}

    private record DomainNameResource(String region, String domainName) implements TaggableResource {}

    /** Parses the API and Stage ARN shapes accepted by API Gateway v2 tagging operations. */
    private TaggableResource parseTaggableResource(String resourceArn) {
        if (resourceArn == null || resourceArn.isBlank()) {
            throw new AwsException("BadRequestException", "ResourceArn must not be blank", 400);
        }
        AwsArnUtils.Arn arn;
        try {
            arn = AwsArnUtils.parse(resourceArn);
        } catch (IllegalArgumentException e) {
            throw new AwsException("BadRequestException",
                    "Invalid ResourceArn format: " + resourceArn, 400);
        }
        if (!"apigateway".equals(arn.service()) || !arn.accountId().isEmpty()) {
            throw invalidTaggableResourceArn(resourceArn);
        }

        String[] segments = arn.resource().split("/", -1);
        if (segments.length == 3
                && segments[0].isEmpty()
                && "apis".equals(segments[1])
                && !segments[2].isEmpty()) {
            return new ApiResource(arn.region(), segments[2]);
        }
        if (segments.length == 5
                && segments[0].isEmpty()
                && "apis".equals(segments[1])
                && !segments[2].isEmpty()
                && "stages".equals(segments[3])
                && !segments[4].isEmpty()) {
            return new StageResource(arn.region(), segments[2], segments[4]);
        }
        if (segments.length == 3
                && segments[0].isEmpty()
                && "domainnames".equals(segments[1])
                && !segments[2].isEmpty()) {
            return new DomainNameResource(arn.region(), segments[2]);
        }
        throw invalidTaggableResourceArn(resourceArn);
    }

    private AwsException invalidTaggableResourceArn(String resourceArn) {
        return new AwsException("BadRequestException",
                "Unsupported API Gateway v2 ResourceArn: " + resourceArn, 400);
    }

    private Map<String, String> readTags(TaggableResource resource) {
        Map<String, String> tags = switch (resource) {
            case ApiResource api -> getApi(api.region(), api.apiId()).getTags();
            case StageResource stage -> getStage(stage.region(), stage.apiId(), stage.stageName()).getTags();
            case DomainNameResource domain -> getDomainName(domain.region(), domain.domainName()).getTags();
        };
        return tags == null ? new HashMap<>() : new HashMap<>(tags);
    }

    private void writeTags(TaggableResource resource, Map<String, String> tags) {
        switch (resource) {
            case ApiResource ref -> {
                Api api = getApi(ref.region(), ref.apiId());
                api.setTags(tags);
                apiStore.put(apiKey(ref.region(), ref.apiId()), api);
            }
            case StageResource ref -> {
                Stage stage = getStage(ref.region(), ref.apiId(), ref.stageName());
                stage.setTags(tags);
                stage.setLastUpdatedDate(System.currentTimeMillis());
                stageStore.put(stageKey(ref.region(), ref.apiId(), ref.stageName()), stage);
            }
            case DomainNameResource ref -> {
                DomainName domain = getDomainName(ref.region(), ref.domainName());
                domain.setTags(tags);
                domainNameStore.put(domainNameKey(ref.region(), ref.domainName()), domain);
            }
        }
    }

    public void tagResource(String resourceArn, Map<String, String> tags) {
        ReservedTags.rejectApiGatewayReservedTagsOnUpdate(tags);
        TaggableResource resource = parseTaggableResource(resourceArn);
        if (tags != null && !tags.isEmpty()) {
            Map<String, String> updated = readTags(resource);
            updated.putAll(tags);
            writeTags(resource, updated);
        } else {
            readTags(resource);
        }
    }

    public void untagResource(String resourceArn, List<String> tagKeys) {
        TaggableResource resource = parseTaggableResource(resourceArn);
        Map<String, String> updated = readTags(resource);
        if (tagKeys != null) {
            tagKeys.forEach(updated::remove);
        }
        writeTags(resource, updated);
    }

    public Map<String, String> getTags(String resourceArn) {
        return readTags(parseTaggableResource(resourceArn));
    }

    // ──────────────────────────── Key helpers ────────────────────────────

    private String apiKey(String region, String apiId) {
        return region + "::" + apiId;
    }

    private String routeKey(String region, String apiId, String routeId) {
        return region + "::" + apiId + "::" + routeId;
    }

    private String integrationKey(String region, String apiId, String integrationId) {
        return region + "::" + apiId + "::" + integrationId;
    }

    private String authorizerKey(String region, String apiId, String authorizerId) {
        return region + "::" + apiId + "::" + authorizerId;
    }

    private String stageKey(String region, String apiId, String stageName) {
        return region + "::" + apiId + "::" + stageName;
    }

    private String deploymentKey(String region, String apiId, String deploymentId) {
        return region + "::" + apiId + "::" + deploymentId;
    }

    private String routeResponseKey(String region, String apiId, String routeId, String routeResponseId) {
        return region + "::" + apiId + "::" + routeId + "::" + routeResponseId;
    }

    private String integrationResponseKey(String region, String apiId, String integrationId, String integrationResponseId) {
        return region + "::" + apiId + "::" + integrationId + "::" + integrationResponseId;
    }

    private String modelKey(String region, String apiId, String modelId) {
        return region + "::" + apiId + "::" + modelId;
    }

    private String vpcLinkKey(String region, String vpcLinkId) {
        return region + "::" + vpcLinkId;
    }

    private String domainNameKey(String region, String domainName) {
        return region + "::" + domainName;
    }

    private String apiMappingKey(String region, String domainName, String apiMappingId) {
        return region + "::" + domainName + "::" + apiMappingId;
    }

    private static String shortId(int length) {
        return UUID.randomUUID().toString().replace("-", "").substring(0, length);
    }
}
