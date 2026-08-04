package io.github.hectorvent.floci.services.lambda;

import io.github.hectorvent.floci.core.common.RegionResolver;
import io.github.hectorvent.floci.core.storage.AccountAwareStorageBackend;
import io.github.hectorvent.floci.core.storage.InMemoryStorage;
import io.github.hectorvent.floci.core.storage.StorageBackend;
import io.github.hectorvent.floci.services.lambda.model.EventSourceMapping;
import io.github.hectorvent.floci.services.lambda.zip.CodeStore;
import io.github.hectorvent.floci.services.lambda.zip.ZipExtractor;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;

class LambdaEsmTagPersistenceTest {

    private static final String REGION = "us-east-1";
    private static final String ACCOUNT_ID = "000000000000";
    private static final String UUID = "7a1e4850-1323-4b74-838e-15e7919a31a0";
    private static final String FUNCTION_NAME = "legacy-esm-function";
    private static final String FUNCTION_ARN =
            "arn:aws:lambda:" + REGION + ":" + ACCOUNT_ID + ":function:" + FUNCTION_NAME;
    private static final String MAPPING_ARN =
            "arn:aws:lambda:" + REGION + ":" + ACCOUNT_ID + ":event-source-mapping:" + UUID;

    @Test
    void legacyIdentityAndTagsPersistAcrossServiceReload() {
        StorageBackend<String, EventSourceMapping> backend =
                new AccountAwareStorageBackend<>(new InMemoryStorage<>(), null, ACCOUNT_ID);
        EsmStore firstStore = new EsmStore(backend);

        EventSourceMapping legacy = new EventSourceMapping();
        legacy.setUuid(UUID);
        legacy.setFunctionName(FUNCTION_NAME);
        legacy.setFunctionArn(FUNCTION_ARN);
        legacy.setEventSourceMappingArn(
                "arn:aws:lambda:" + REGION + ":null:event-source-mapping:" + UUID);
        legacy.setRegion(REGION);
        legacy.setAccountId(null);
        legacy.setTags(new HashMap<>(Map.of("alchemy::id", "legacy-esm")));
        firstStore.save(legacy);

        LambdaService first = service(firstStore);
        EventSourceMapping migrated = first.getEventSourceMapping(UUID);
        assertEquals(ACCOUNT_ID, migrated.getAccountId());
        assertEquals(MAPPING_ARN, migrated.getEventSourceMappingArn());
        assertFalse(migrated.getEventSourceMappingArn().contains(":null:"));
        assertEquals(Map.of("alchemy::id", "legacy-esm"), first.listTags(MAPPING_ARN));

        first.tagResource(MAPPING_ARN, Map.of("owner", "alchemy"));

        EsmStore reloadedStore = new EsmStore(backend);
        LambdaService reloaded = service(reloadedStore);
        assertEquals(Map.of("alchemy::id", "legacy-esm", "owner", "alchemy"),
                reloaded.listTags(MAPPING_ARN));

        reloaded.untagResource(MAPPING_ARN, List.of("alchemy::id"));
        assertEquals(Map.of("owner", "alchemy"), service(new EsmStore(backend)).listTags(MAPPING_ARN));
        assertNull(new EsmStore(backend).get(UUID).orElseThrow().getTags().get("alchemy::id"));
    }

    @Test
    void legacyConflatedFunctionArnMigratesToDistinctResources() {
        StorageBackend<String, EventSourceMapping> backend =
                new AccountAwareStorageBackend<>(new InMemoryStorage<>(), null, ACCOUNT_ID);
        EsmStore store = new EsmStore(backend);

        EventSourceMapping legacy = new EventSourceMapping();
        legacy.setUuid(UUID);
        legacy.setFunctionName(FUNCTION_NAME);
        legacy.setFunctionArn(MAPPING_ARN);
        legacy.setEventSourceMappingArn(null);
        legacy.setRegion(null);
        legacy.setAccountId(null);
        legacy.setTags(null);
        store.save(legacy);

        EventSourceMapping migrated = service(store).getEventSourceMapping(UUID);
        assertEquals(ACCOUNT_ID, migrated.getAccountId());
        assertEquals(REGION, migrated.getRegion());
        assertEquals(FUNCTION_ARN, migrated.getFunctionArn());
        assertEquals(MAPPING_ARN, migrated.getEventSourceMappingArn());
        assertEquals(Map.of(), migrated.getTags());
        assertEquals(Map.of(), service(new EsmStore(backend)).listTags(MAPPING_ARN));
    }

    private static LambdaService service(EsmStore esmStore) {
        return new LambdaService(
                mock(LambdaFunctionStore.class),
                null,
                new LambdaConcurrencyLimiter(),
                new WarmPool(),
                new CodeStore(Path.of("target/test-data/lambda-esm-tags")),
                new ZipExtractor(),
                null,
                new RegionResolver(REGION, ACCOUNT_ID),
                esmStore,
                null,
                null,
                null,
                null,
                null,
                null,
                null);
    }
}
