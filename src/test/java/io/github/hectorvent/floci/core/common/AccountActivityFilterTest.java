package io.github.hectorvent.floci.core.common;

import io.github.hectorvent.floci.lifecycle.EmulatorInfoController;
import jakarta.ws.rs.container.CompletionCallback;
import jakarta.ws.rs.container.ResourceInfo;
import org.jboss.resteasy.reactive.server.spi.ResteasyReactiveContainerRequestContext;
import org.jboss.resteasy.reactive.server.spi.ServerRequestContext;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class AccountActivityFilterTest {

    private static final String TARGET = "111111111111";

    private final AccountActivity activity = new AccountActivity();
    private final RequestContext requestContext = new RequestContext();
    private final AccountActivityFilter filter = new AccountActivityFilter(requestContext, activity);
    private final ResteasyReactiveContainerRequestContext request = mock(ResteasyReactiveContainerRequestContext.class);

    @Test
    void releaseFollowsRequestCompletionEvenOnFailure() {
        filter.resourceInfo = mock(ResourceInfo.class);
        requestContext.setAccountId(TARGET);
        ServerRequestContext server = mock(ServerRequestContext.class);
        when(request.getServerRequestContext()).thenReturn(server);

        filter.filter(request);

        assertNull(activity.tryReclaim(TARGET));
        ArgumentCaptor<CompletionCallback> completion = ArgumentCaptor.forClass(CompletionCallback.class);
        verify(server).registerCompletionCallback(completion.capture());
        completion.getValue().onComplete(new IllegalStateException("aborted response"));
        try (AccountActivity.Permit reclaim = activity.tryReclaim(TARGET)) {
            assertNotNull(reclaim);
            assertEquals(409, assertThrows(AwsException.class, () -> filter.filter(request)).getHttpStatus());
        }
    }

    @Test
    void emulatorStateRoutesCanObserveAndHoldTheirOwnGate() {
        filter.resourceInfo = mock(ResourceInfo.class);
        when(filter.resourceInfo.getResourceClass()).thenAnswer(ignored -> EmulatorInfoController.class);
        requestContext.setAccountId(TARGET);
        try (AccountActivity.Permit reclaim = activity.tryReclaim(TARGET)) {
            assertNotNull(reclaim);
            assertDoesNotThrow(() -> filter.filter(request));
            verifyNoInteractions(request);
        }
    }
}
