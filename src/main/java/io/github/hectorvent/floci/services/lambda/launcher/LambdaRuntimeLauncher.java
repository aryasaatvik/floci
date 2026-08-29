package io.github.hectorvent.floci.services.lambda.launcher;

import io.github.hectorvent.floci.services.lambda.model.LambdaFunction;

/**
 * Starts and stops Lambda execution environments. Implementations back each
 * environment with a concrete runtime: a Docker container ({@link ContainerLauncher})
 * or a Kubernetes pod ({@code KubernetesPodLauncher}). Selected at startup by
 * {@code floci.services.lambda.executor}.
 */
public interface LambdaRuntimeLauncher {

    /**
     * Result of a lifecycle probe. An unknown result means the launcher could not establish
     * whether the underlying environment still exists; callers must retain its physical
     * ownership rather than treating an observation failure as a clean teardown.
     */
    enum Liveness {
        ALIVE,
        DEAD,
        UNKNOWN
    }

    ContainerHandle launch(LambdaFunction fn);

    void stop(ContainerHandle handle);

    /**
     * Probes the underlying environment. Implementations must return {@link Liveness#DEAD} only
     * after confirming that the environment is gone, not merely stopped or temporarily
     * unreachable.
     */
    Liveness liveness(ContainerHandle handle);

    /**
     * Legacy running-state probe retained for launcher consumers outside the warm pool.
     * The tri-state {@link #liveness(ContainerHandle)} probe is the ownership-safe API.
     */
    boolean isAlive(ContainerHandle handle);
}
