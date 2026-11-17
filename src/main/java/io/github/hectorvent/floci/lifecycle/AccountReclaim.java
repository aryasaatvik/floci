package io.github.hectorvent.floci.lifecycle;

import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.core.common.AccountActivity;
import io.github.hectorvent.floci.core.common.AwsException;
import io.github.hectorvent.floci.core.common.FileTrees;
import io.github.hectorvent.floci.core.common.Resettable;
import io.github.hectorvent.floci.core.common.docker.ContainerLifecycleManager;
import io.github.hectorvent.floci.core.common.docker.ContainerStorageHelper;
import io.github.hectorvent.floci.core.storage.StorageFactory;
import io.github.hectorvent.floci.services.lambda.LambdaFunctionStore;
import io.github.hectorvent.floci.services.lambda.zip.CodeStore;
import io.github.hectorvent.floci.services.s3.S3Service;
import io.quarkus.arc.Arc;
import io.quarkus.arc.InjectableContext;
import io.quarkus.runtime.annotations.RegisterForReflection;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.spi.Bean;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

@ApplicationScoped
public class AccountReclaim {

    private static final Logger LOG = Logger.getLogger(AccountReclaim.class);

    private final AccountActivity activity;
    private final EmulatorConfig config;
    private final Supplier<List<Resettable>> liveServices;
    private final ContainerLifecycleManager containers;
    private final StorageFactory storage;

    @Inject
    AccountReclaim(AccountActivity activity, EmulatorConfig config, Instance<Resettable> resettables,
                   ContainerLifecycleManager containers, StorageFactory storage) {
        this(activity, config, () -> existingServices(resettables, Arc.container()::getActiveContext), containers, storage);
    }

    AccountReclaim(AccountActivity activity, EmulatorConfig config, Supplier<List<Resettable>> liveServices,
                   ContainerLifecycleManager containers, StorageFactory storage) {
        this.activity = activity;
        this.config = config;
        this.liveServices = liveServices;
        this.containers = containers;
        this.storage = storage;
    }

    Preview preview(String accountId) throws IOException {
        Path persistentRoot = Path.of(config.storage().persistentPath());
        Path codeRoot = Path.of(config.services().lambda().codePath());
        long s3Bytes = S3Service.storesPayloadFiles(config)
                ? FileTrees.regularFileBytes(persistentRoot, S3Service.accountDataPath(S3Service.dataRoot(config), accountId))
                : 0;
        long lambdaCodeBytes = FileTrees.regularFileBytes(codeRoot, CodeStore.accountCodePath(codeRoot, accountId));

        boolean busy = activity.isBusy(accountId) || isBusy(accountId, liveServices.get()) || hasContainers(accountId);
        return new Preview(accountId, !busy, busy ? "account-busy" : null, s3Bytes, lambdaCodeBytes);
    }

    boolean reclaim(String accountId) throws IOException {
        try (AccountActivity.Permit permit = activity.tryReclaim(accountId)) {
            if (permit == null) {
                return false;
            }
            List<Resettable> services = liveServices.get();
            if (isBusy(accountId, services) || hasContainers(accountId)) {
                return false;
            }
            Path persistentRoot = Path.of(config.storage().persistentPath());
            if (S3Service.storesPayloadFiles(config)) {
                FileTrees.checkScopedRoot(persistentRoot, S3Service.accountDataPath(S3Service.dataRoot(config), accountId));
            }
            Path codeRoot = Path.of(config.services().lambda().codePath());
            FileTrees.checkScopedRoot(codeRoot, CodeStore.accountCodePath(codeRoot, accountId));

            StorageFactory.PreparedAccountReclaim clearStorage = storage.prepareAccountReclaim(accountId);
            services = liveServices.get();
            StorageFactory.PreparedAccountReclaim coldCode = null;
            StorageFactory.PreparedAccountReclaim coldS3 = null;
            try {
                if (!codeStoreReady(services)) {
                    coldCode = CodeStore.prepareDormantAccountClear(config, storage, accountId);
                }
                if (services.stream().noneMatch(S3Service.class::isInstance)) {
                    coldS3 = S3Service.prepareDormantAccountClear(config, storage, accountId);
                }
            } catch (AwsException failure) {
                if (failure.getHttpStatus() != 409) {
                    throw failure;
                }
                return false;
            }
            List<Resettable> current = liveServices.get();
            if (isBusy(accountId, current) || hasContainers(accountId)) {
                return false;
            }
            boolean codeReady = codeStoreReady(current);
            for (Resettable service : current) {
                if (codeReady || !(service instanceof CodeStore)) {
                    service.clearAccount(accountId);
                }
            }
            if (coldCode != null && !codeReady) {
                coldCode.apply();
            }
            if (coldS3 != null && current.stream().noneMatch(S3Service.class::isInstance)) {
                coldS3.apply();
            }
            clearStorage.apply();
            return true;
        }
    }

    private static boolean codeStoreReady(List<Resettable> services) {
        return services.stream().anyMatch(CodeStore.class::isInstance)
                && services.stream().anyMatch(LambdaFunctionStore.class::isInstance);
    }

    private static boolean isBusy(String accountId, List<Resettable> services) {
        // CodeStore delegates legacy path validation to LambdaFunctionStore. Checking through a
        // dormant dependency could open its backend. Reclaim checks dormant metadata before clearing.
        boolean functionsLive = services.stream().anyMatch(LambdaFunctionStore.class::isInstance);
        for (Resettable service : services) {
            if (service instanceof CodeStore && !functionsLive) {
                continue;
            }
            try {
                service.checkAccountReset(accountId);
            } catch (AwsException failure) {
                if (failure.getHttpStatus() != 409) {
                    throw failure;
                }
                return true;
            }
        }
        return false;
    }

    static List<Resettable> existingServices(Instance<Resettable> resettables,
                                           Function<Class<? extends Annotation>, InjectableContext> contextForScope) {
        List<Resettable> services = new ArrayList<>();
        for (Instance.Handle<Resettable> handle : resettables.handles()) {
            Bean<Resettable> bean = handle.getBean();
            InjectableContext context = contextForScope.apply(bean.getScope());
            // Context#get without a creational context reads only an existing instance.
            if (context != null) {
                Resettable service = context.get(bean);
                if (service != null) {
                    services.add(service);
                }
            }
        }
        return services;
    }

    private boolean hasContainers(String accountId) {
        try {
            String owner = ContainerStorageHelper.ownerIdentity(config);
            String namespace = ContainerStorageHelper.resourceNamespace(config);
            return containers.listContainersByLabels(Map.of(ContainerStorageHelper.ACCOUNT_LABEL, accountId,
                    "floci", "true")).stream().anyMatch(container -> {
                        Map<String, String> labels = container.getLabels();
                        String stampedOwner = ContainerStorageHelper.labelValue(labels, ContainerStorageHelper.OWNER_LABEL);
                        return stampedOwner == null ? namespace.equals(labels.getOrDefault("floci_namespace", ""))
                                : owner.equals(stampedOwner);
                    });
        } catch (RuntimeException failure) {
            LOG.warnv("Could not inspect container dependencies for account {0}: {1}",
                    accountId, failure.getClass().getSimpleName());
            throw new AwsException("ServiceUnavailable", "Could not inspect account container dependencies", 503);
        }
    }

    @RegisterForReflection
    public record Preview(String accountId, boolean eligible, String blocker, long s3Bytes, long lambdaCodeBytes) {}
}
