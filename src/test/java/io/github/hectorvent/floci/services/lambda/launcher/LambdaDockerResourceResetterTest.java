package io.github.hectorvent.floci.services.lambda.launcher;

import io.github.hectorvent.floci.config.EmulatorConfig;
import jakarta.enterprise.inject.Instance;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class LambdaDockerResourceResetterTest {

    @Mock EmulatorConfig config;
    @Mock EmulatorConfig.ServicesConfig services;
    @Mock EmulatorConfig.LambdaServiceConfig lambda;
    @Mock Instance<LambdaDockerResourceReconciler> reconcilers;
    @Mock LambdaDockerResourceReconciler reconciler;

    private LambdaDockerResourceResetter resetter;

    @BeforeEach
    void setUp() {
        when(config.services()).thenReturn(services);
        when(services.lambda()).thenReturn(lambda);
        resetter = new LambdaDockerResourceResetter(config, reconcilers);
    }

    @Test
    void dockerResetLazilyClearsOwnedResources() {
        when(lambda.enabled()).thenReturn(true);
        when(lambda.executor()).thenReturn("docker");
        when(reconcilers.get()).thenReturn(reconciler);

        resetter.clear();

        verify(reconcilers).get();
        verify(reconciler).clearOwnedResources();
    }

    @Test
    void kubernetesResetDoesNotInstantiateDockerReconciler() {
        when(lambda.enabled()).thenReturn(true);
        when(lambda.executor()).thenReturn("kubernetes");

        resetter.clear();

        verify(reconcilers, never()).get();
        verify(reconciler, never()).clearOwnedResources();
    }

    @Test
    void disabledLambdaDoesNotInstantiateDockerReconciler() {
        when(lambda.enabled()).thenReturn(false);

        resetter.clear();

        verify(reconcilers, never()).get();
        verify(reconciler, never()).clearOwnedResources();
    }
}
