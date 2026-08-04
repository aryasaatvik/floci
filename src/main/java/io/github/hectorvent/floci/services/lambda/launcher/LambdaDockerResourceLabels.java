package io.github.hectorvent.floci.services.lambda.launcher;

import io.github.hectorvent.floci.services.lambda.model.LambdaFunction;

import java.util.HashMap;
import java.util.Map;

/** Ownership labels for Docker resources created by the Lambda emulator. */
final class LambdaDockerResourceLabels {

    static final String MANAGED = "io.floci.managed";
    static final String INSTANCE = "io.floci.instance";
    static final String SERVICE = "io.floci.service";
    static final String KIND = "io.floci.kind";
    static final String ACCOUNT = "io.floci.aws.account";
    static final String REGION = "io.floci.aws.region";
    static final String FUNCTION = "io.floci.lambda.function";
    static final String VERSION = "io.floci.lambda.version";
    static final String CODE = "io.floci.lambda.code";

    static final String EXECUTION = "execution";
    static final String CODE_VOLUME = "code";
    static final String CODE_POPULATOR = "code-populator";

    private LambdaDockerResourceLabels() {}

    static Map<String, String> owner(String instanceId) {
        return Map.of(
                MANAGED, "true",
                INSTANCE, instanceId,
                SERVICE, "lambda");
    }

    static Map<String, String> forFunction(String instanceId, LambdaFunction function, String kind) {
        LambdaExecutionEnvironmentId environment = LambdaExecutionEnvironmentId.from(function);
        Map<String, String> labels = new HashMap<>(owner(instanceId));
        labels.put(KIND, kind);
        labels.put(ACCOUNT, environment.accountId());
        labels.put(REGION, environment.region());
        labels.put(FUNCTION, environment.functionName());
        labels.put(VERSION, environment.version());
        labels.put(CODE, ContainerLauncher.codeIdentity(function));
        return Map.copyOf(labels);
    }
}
