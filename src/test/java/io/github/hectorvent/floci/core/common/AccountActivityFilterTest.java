package io.github.hectorvent.floci.core.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.hectorvent.floci.lifecycle.EmulatorInfoController;
import io.github.hectorvent.floci.services.apigateway.ApiGatewayExecuteController;
import io.github.hectorvent.floci.services.apigateway.ApiGatewayExecuteRouteContext;
import io.github.hectorvent.floci.services.apigateway.ApiGatewayService;
import jakarta.ws.rs.container.CompletionCallback;
import jakarta.ws.rs.container.ResourceInfo;
import jakarta.ws.rs.core.HttpHeaders;
import org.jboss.resteasy.reactive.server.spi.ResteasyReactiveContainerRequestContext;
import org.jboss.resteasy.reactive.server.spi.ServerRequestContext;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Optional;

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
    void anOwnerDiscoveredDuringRoutingMustBeAdmittedBeforeSwitchingAndUntilCompletion() {
        String caller = "000000000000";
        filter.resourceInfo = mock(ResourceInfo.class);
        requestContext.setAccountId(caller);
        ServerRequestContext server = mock(ServerRequestContext.class);
        when(request.getServerRequestContext()).thenReturn(server);
        filter.filter(request);

        try (AccountActivity.Permit reclaim = activity.tryReclaim(TARGET)) {
            assertNotNull(reclaim);
            assertEquals(409, assertThrows(AwsException.class, () -> requestContext.setAccountId(TARGET)).getHttpStatus());
            assertEquals(caller, requestContext.getAccountId());
        }

        requestContext.setAccountId(TARGET);
        requestContext.setAccountId(caller);
        requestContext.setAccountId(TARGET);
        assertNull(activity.tryReclaim(TARGET));
        ArgumentCaptor<CompletionCallback> completion = ArgumentCaptor.forClass(CompletionCallback.class);
        verify(server, times(2)).registerCompletionCallback(completion.capture());
        completion.getAllValues().forEach(callback -> callback.onComplete(null));
        assertFalse(activity.isBusy(TARGET));
        assertFalse(activity.isBusy(caller));
    }

    @Test
    void unsignedExecuteApiRequestCannotDispatchWhileTheApiOwnerIsReclaiming() {
        String caller = "000000000000";
        String region = "us-east-1";
        ApiGatewayService gateway = mock(ApiGatewayService.class);
        HttpHeaders headers = mock(HttpHeaders.class);
        RegionResolver regions = mock(RegionResolver.class);
        when(regions.resolveRegion(headers)).thenReturn(region);
        when(gateway.findRestApiOwner("test-api", null, region))
                .thenReturn(Optional.of(new ApiGatewayService.ApiOwner(TARGET, region)));
        ApiGatewayExecuteController controller = new ApiGatewayExecuteController(
                gateway, null, null, null, regions, new ObjectMapper(), null,
                null, null, null, null, new ApiGatewayExecuteRouteContext(), null, requestContext, null, null, null);
        filter.resourceInfo = mock(ResourceInfo.class);
        requestContext.setAccountId(caller);
        when(request.getServerRequestContext()).thenReturn(mock(ServerRequestContext.class));
        filter.filter(request);

        try (AccountActivity.Permit reclaim = activity.tryReclaim(TARGET)) {
            assertNotNull(reclaim);
            AwsException refused = assertThrows(AwsException.class,
                    () -> controller.handleGet(headers, null, "test-api", "live", "hello"));
            assertEquals(409, refused.getHttpStatus());
            assertEquals(caller, requestContext.getAccountId());
            verify(gateway, never()).getRestApi(region, "test-api");
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
