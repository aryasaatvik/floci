package io.github.hectorvent.floci.services.lambda;

import io.github.hectorvent.floci.core.common.AwsException;
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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

class LambdaEsmTagPersistenceTest {

    private static final String REGION = "us-east-1";
    private static final String ACCOUNT_ID = "000000000000";
    private static final String UUID = "7a1e4850-1323-4b74-838e-15e7919a31a0";
    private static final String MAPPING_ARN =
            "arn:aws:lambda:" + REGION + ":" + ACCOUNT_ID + ":event-source-mapping:" + UUID;

    @Test
    void identityAndTagMutationsPersistAcrossServiceReload() {
        StorageBackend<String, EventSourceMapping> backend =
                new AccountAwareStorageBackend<>(new InMemoryStorage<>(), null, ACCOUNT_ID);
        EsmStore store = new EsmStore(backend);
        EventSourceMapping mapping = mapping();
        mapping.setEventSourceMappingArn(MAPPING_ARN);
        mapping.setTags(new HashMap<>(Map.of("project", "billing")));
        store.save(mapping);

        LambdaService first = service(store);
        assertEquals(Map.of("project", "billing"), first.listTags(MAPPING_ARN));

        first.tagResource(MAPPING_ARN, Map.of("owner", "platform"));

        LambdaService reloaded = service(new EsmStore(backend));
        assertEquals(Map.of("project", "billing", "owner", "platform"),
                reloaded.listTags(MAPPING_ARN));

        reloaded.untagResource(MAPPING_ARN, List.of("project"));
        assertEquals(Map.of("owner", "platform"),
                service(new EsmStore(backend)).listTags(MAPPING_ARN));
    }

    @Test
    void existingMappingWithoutArnReceivesAndPersistsCanonicalIdentity() {
        StorageBackend<String, EventSourceMapping> backend = new InMemoryStorage<>();
        EsmStore store = new EsmStore(backend);
        store.save(mapping());

        EventSourceMapping migrated = service(store).getEventSourceMapping(UUID);

        assertEquals(MAPPING_ARN, migrated.getEventSourceMappingArn());
        assertEquals(MAPPING_ARN,
                new EsmStore(backend).get(UUID).orElseThrow().getEventSourceMappingArn());
    }

    @Test
    void mappingTagsRequireThePersistedArnIdentity() {
        EsmStore store = new EsmStore(new InMemoryStorage<>());
        EventSourceMapping mapping = mapping();
        mapping.setEventSourceMappingArn(MAPPING_ARN);
        store.save(mapping);

        AwsException error = assertThrows(AwsException.class, () -> service(store).listTags(
                "arn:aws:lambda:" + REGION + ":000000000001:event-source-mapping:" + UUID));

        assertEquals("ResourceNotFoundException", error.getErrorCode());
    }

    private static EventSourceMapping mapping() {
        EventSourceMapping mapping = new EventSourceMapping();
        mapping.setUuid(UUID);
        mapping.setFunctionName("esm-persistence-function");
        mapping.setFunctionArn("arn:aws:lambda:" + REGION + ":" + ACCOUNT_ID
                + ":function:esm-persistence-function");
        mapping.setRegion(REGION);
        mapping.setAccountId(ACCOUNT_ID);
        return mapping;
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
