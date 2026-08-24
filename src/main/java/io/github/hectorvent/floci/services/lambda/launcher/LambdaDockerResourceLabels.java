package io.github.hectorvent.floci.services.lambda.launcher;

import io.github.hectorvent.floci.services.lambda.model.LambdaFunction;

import java.util.HashMap;
import java.util.Map;

/** Exact ownership labels for Docker resources created by the Lambda executor. */
final class LambdaDockerResourceLabels {

    static final String MANAGED = "io.floci.managed";
    static final String INSTANCE = "io.floci.instance";
    static final String SERVICE = "io.floci.service";
    static final String KIND = "io.floci.kind";
    static final String FUNCTION = "io.floci.lambda.function";
    static final String CODE = "io.floci.lambda.code";

    static final String EXECUTION = "execution";
    static final String CODE_VOLUME = "code";
    static final String CODE_POPULATOR = "code-populator";

    private LambdaDockerResourceLabels() {
    }

    static Map<String, String> owner(String instanceId) {
        return Map.of(
                MANAGED, "true",
                INSTANCE, instanceId,
                SERVICE, "lambda");
    }

    static Map<String, String> ownedCodeVolume(String instanceId) {
        Map<String, String> labels = new HashMap<>(owner(instanceId));
        labels.put(KIND, CODE_VOLUME);
        return Map.copyOf(labels);
    }

    static Map<String, String> forFunction(String instanceId, LambdaFunction function, String kind) {
        Map<String, String> labels = new HashMap<>(owner(instanceId));
        labels.put(KIND, kind);
        labels.put(FUNCTION, function.getFunctionName());
        labels.put(CODE, ContainerLauncher.codeIdentity(function));
        return Map.copyOf(labels);
    }
}
