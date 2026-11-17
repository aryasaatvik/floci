package io.github.hectorvent.floci.core.common;

import io.github.hectorvent.floci.lifecycle.EmulatorInfoController;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.Priorities;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.ResourceInfo;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.ext.Provider;
import org.jboss.resteasy.reactive.server.spi.ResteasyReactiveContainerRequestContext;

import java.util.HashSet;
import java.util.Set;

/** Keep a request admitted until its response, including a streamed response, has completed. */
@Provider
@ApplicationScoped
@Priority(Priorities.AUTHENTICATION - 50)
public class AccountActivityFilter implements ContainerRequestFilter {

    @Context
    ResourceInfo resourceInfo;

    private final RequestContext requestContext;
    private final AccountActivity activity;

    @Inject
    AccountActivityFilter(RequestContext requestContext, AccountActivity activity) {
        this.requestContext = requestContext;
        this.activity = activity;
    }

    @Override
    public void filter(ContainerRequestContext context) {
        if (EmulatorInfoController.class.equals(resourceInfo.getResourceClass())) {
            return;
        }
        ResteasyReactiveContainerRequestContext request = (ResteasyReactiveContainerRequestContext) context;
        Set<String> admitted = new HashSet<>();
        // Host routes and integrations can select a resource owner after the signing account is
        // known. Admit each owner before changing request context, for the same response lifetime.
        requestContext.admitAccountsWith(account -> {
            if (!admitted.contains(account)) {
                AccountActivity.Permit permit = activity.beginWork(account);
                try {
                    request.getServerRequestContext().registerCompletionCallback(failure -> permit.close());
                    admitted.add(account);
                } catch (RuntimeException failure) {
                    permit.close();
                    throw failure;
                }
            }
        });
    }
}
