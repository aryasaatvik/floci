package io.github.hectorvent.floci.services.apigatewayv2.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection
@JsonIgnoreProperties(ignoreUnknown = true)
public class ApiMapping {
    private String apiMappingId;
    private String apiId;
    private String apiMappingKey;
    private String stage;

    public String getApiMappingId() { return apiMappingId; }
    public void setApiMappingId(String apiMappingId) { this.apiMappingId = apiMappingId; }

    public String getApiId() { return apiId; }
    public void setApiId(String apiId) { this.apiId = apiId; }

    public String getApiMappingKey() { return apiMappingKey; }
    public void setApiMappingKey(String apiMappingKey) { this.apiMappingKey = apiMappingKey; }

    public String getStage() { return stage; }
    public void setStage(String stage) { this.stage = stage; }
}
