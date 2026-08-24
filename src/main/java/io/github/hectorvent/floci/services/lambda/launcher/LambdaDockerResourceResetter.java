package io.github.hectorvent.floci.services.lambda.launcher;

import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.core.common.Resettable;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

/** Lazily connects emulator reset to Lambda's Docker resource cleanup. */
@ApplicationScoped
public class LambdaDockerResourceResetter implements Resettable {

    private final EmulatorConfig config;
    private final Instance<LambdaDockerResourceReconciler> reconcilers;

    @Inject
    public LambdaDockerResourceResetter(EmulatorConfig config,
                                        Instance<LambdaDockerResourceReconciler> reconcilers) {
        this.config = config;
        this.reconcilers = reconcilers;
    }

    @Override
    public void clear() {
        if (config.services().lambda().enabled()
                && "docker".equalsIgnoreCase(config.services().lambda().executor())) {
            reconcilers.get().clearOwnedResources();
        }
    }
}
