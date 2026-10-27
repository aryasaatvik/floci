package io.github.hectorvent.floci.services.lambda.launcher;

import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.core.common.docker.ContainerStorageHelper;
import io.github.hectorvent.floci.services.lambda.model.LambdaFunction;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Labels on the Docker containers and volumes the Lambda launcher creates.
 *
 * <p>Each one carries the {@code io.floci.*} resource identity ({@link ContainerStorageHelper}),
 * the {@link ContainerStorageHelper#OWNER_LABEL owner} of the Floci deployment that created it, and
 * a {@link ContainerStorageHelper#COMPONENT_LABEL component}: an {@link #EXECUTION} environment, a
 * {@link #CODE_VOLUME} or the {@link #CODE_POPULATOR} container that fills one. The
 * {@code io.floci.lambda.*} labels name the function, its code and its execution environment, so
 * a startup sweep and tooling outside Floci can attribute a resource without parsing its name.
 */
public final class LambdaDockerResourceLabels {

    /** The function's name. */
    public static final String FUNCTION_LABEL = "io.floci.lambda.function";
    /** The function's {@code CodeSha256}, or its last-modified time when it has none. */
    public static final String CODE_LABEL = "io.floci.lambda.code";
    /** The execution environment's key, {@link #environmentKey}. */
    public static final String ENVIRONMENT_LABEL = "io.floci.lambda.environment";

    public static final String EXECUTION = "execution";
    public static final String CODE_VOLUME = "code-volume";
    public static final String CODE_POPULATOR = "code-populator";

    private static final String SERVICE = "lambda";

    private LambdaDockerResourceLabels() {
    }

    /** The labels every Lambda resource of this Floci deployment carries. */
    public static Map<String, String> owner(EmulatorConfig config) {
        return Map.of(ContainerStorageHelper.SERVICE_LABEL, SERVICE,
                ContainerStorageHelper.OWNER_LABEL, ContainerStorageHelper.ownerIdentity(config));
    }

    static Map<String, String> forFunction(EmulatorConfig config, LambdaFunction fn, String component,
                                           String accountId, String region) {
        Map<String, String> labels = new LinkedHashMap<>(ContainerStorageHelper.resourceIdentityLabels(
                SERVICE, fn.getFunctionName(), accountId, region));
        labels.putAll(owner(config));
        labels.put(ContainerStorageHelper.COMPONENT_LABEL, component);
        labels.put(FUNCTION_LABEL, fn.getFunctionName());
        labels.put(CODE_LABEL, codeIdentity(fn));
        labels.put(ENVIRONMENT_LABEL, environmentKey(fn));
        return labels;
    }

    static String codeIdentity(LambdaFunction fn) {
        String sha = fn.getCodeSha256();
        return sha == null || sha.isBlank() ? Long.toString(fn.getLastModified()) : sha;
    }

    /**
     * The identity of one immutable execution environment: the function version's ARN, or its
     * name and version when it has no ARN. Warm containers are pooled, and counted, per key.
     */
    public static String environmentKey(LambdaFunction fn) {
        String functionArn = fn.getFunctionArn();
        if (functionArn != null && !functionArn.isBlank()) {
            return functionArn;
        }
        return fn.getFunctionName() + ":" + fn.getVersion();
    }
}
