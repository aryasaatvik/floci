package io.github.hectorvent.floci.core.storage;

import com.fasterxml.jackson.core.type.TypeReference;
import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.core.common.ServiceConfigAccess;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class StorageFactoryAccountResetTest {
    private static final String TARGET = "111111111111";
    private static final String OTHER = "222222222222";
    private static final TypeReference<Map<String, String>> TYPE = new TypeReference<>() {};
    @TempDir Path directory;

    @ParameterizedTest
    @ValueSource(strings = {"memory", "persistent", "hybrid", "wal"})
    void purgeKeepsOtherAccountsAndSurvivesReload(String mode) {
        StorageFactory factory = factory(mode);
        AccountAwareStorageBackend<String> store = factory.create("svc", "svc.json", TYPE, WriteProfile.APPEND_HEAVY);
        store.putForAccount(TARGET, "same", "target");
        store.putForAccount(OTHER, "same", "other");
        factory.clearAccount(TARGET);
        assertTrue(store.getForAccount(TARGET, "same").isEmpty());
        assertEquals("other", store.getForAccount(OTHER, "same").orElseThrow());
        factory.shutdownAll();
        if (!"memory".equals(mode)) {
            StorageFactory restarted = factory(mode);
            AccountAwareStorageBackend<String> reloaded = restarted.create("svc", "svc.json", TYPE, WriteProfile.APPEND_HEAVY);
            assertTrue(reloaded.getForAccount(TARGET, "same").isEmpty());
            assertEquals("other", reloaded.getForAccount(OTHER, "same").orElseThrow());
            restarted.shutdownAll();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void purgeReadsOnlyTheLatestStorageModeWithoutReplayingStaleForeignValues(boolean latestPlain) throws Exception {
        Path snapshot = directory.resolve("switched-snapshot.json");
        Path log = directory.resolve("switched.wal");
        Path plain = directory.resolve("switched.json");
        WalStorage<String, String> old = new WalStorage<>(snapshot, log, TYPE, Long.MAX_VALUE);
        old.load();
        old.put(TARGET + "/same", "target");
        old.put(OTHER + "/same", "stale-snapshot");
        old.flush();
        old.put(OTHER + "/same", "journal-value");
        Files.writeString(plain, "{\"111111111111/same\":\"target\",\"222222222222/same\":\"plain-value\"}");
        FileTime selectedTime = FileTime.from(latestPlain ? Instant.now().plusSeconds(60) : Instant.EPOCH);
        Files.setLastModifiedTime(plain, selectedTime);
        // Do not shutdown the old writer: that would compact its log and change the mode-selection evidence.
        // These are offline fixture files; use a copy so the actual writer remains outside the purge directory.
        Path fixture = directory.resolve("fixture");
        Files.createDirectories(fixture);
        Files.copy(snapshot, fixture.resolve(snapshot.getFileName()));
        Files.copy(log, fixture.resolve(log.getFileName()));
        Files.copy(plain, fixture.resolve(plain.getFileName()));
        Files.setLastModifiedTime(fixture.resolve(plain.getFileName()), selectedTime);
        old.shutdown();
        StorageFactory factory = factory(latestPlain ? "persistent" : "wal", fixture);
        factory.clearAccount(TARGET);
        factory.clearAccount(TARGET);
        AccountAwareStorageBackend<String> store = factory.create("svc", "switched.json", TYPE);
        assertTrue(store.getForAccount(TARGET, "same").isEmpty());
        assertEquals(latestPlain ? "plain-value" : "journal-value", store.getForAccount(OTHER, "same").orElseThrow());
        factory.shutdownAll();
    }

    @Test
    void purgeCoversUnopenedWalAndPartitionedStores() {
        StorageFactory first = factory("wal");
        AccountAwareStorageBackend<String> flat = first.create("svc", "unopened.json", TYPE);
        AccountAwareStorageBackend<Map<String, String>> partitioned = first.createPartitioned("svc", "items.json",
                new TypeReference<>() {}, String.class, 2);
        for (String account : new String[]{TARGET, OTHER}) {
            flat.putForAccount(account, "same", account);
            partitioned.putEntryForAccount(account, "table", "id", account);
        }
        first.shutdownAll();
        StorageFactory lazy = factory("wal");
        lazy.clearAccount(TARGET);
        AccountAwareStorageBackend<String> reopened = lazy.create("svc", "unopened.json", TYPE);
        AccountAwareStorageBackend<Map<String, String>> items = lazy.createPartitioned("svc", "items.json",
                new TypeReference<>() {}, String.class, 2);
        assertTrue(reopened.getForAccount(TARGET, "same").isEmpty());
        assertTrue(items.getForAccount(TARGET, "table").isEmpty());
        assertEquals(OTHER, reopened.getForAccount(OTHER, "same").orElseThrow());
        assertEquals(Map.of("id", OTHER), items.getForAccount(OTHER, "table").orElseThrow());
        lazy.shutdownAll();
    }

    @Test
    void defaultAccountOwnsUnprefixedLegacyKeysWithSlashes() throws Exception {
        Files.writeString(directory.resolve("old.json"), "{\"legacy/path\":\"old\",\"222222222222/key\":\"other\"}");
        StorageFactory factory = factory("persistent");
        factory.clearAccount(TARGET);
        AccountAwareStorageBackend<String> store = factory.create("svc", "old.json", TYPE);
        assertTrue(store.scanAllAccountEntries(key -> true).stream().noneMatch(entry -> TARGET.equals(entry.accountId())));
        assertEquals("other", store.getForAccount(OTHER, "key").orElseThrow());
        factory.shutdownAll();
    }

    private StorageFactory factory(String mode) {
        return factory(mode, directory);
    }

    private StorageFactory factory(String mode, Path path) {
        EmulatorConfig config = mock(EmulatorConfig.class, RETURNS_DEEP_STUBS);
        when(config.defaultAccountId()).thenReturn(TARGET);
        when(config.storage().persistentPath()).thenReturn(path.toString());
        when(config.storage().wal().compactionIntervalMs()).thenReturn(60_000L);
        ServiceConfigAccess access = mock(ServiceConfigAccess.class);
        when(access.storageMode(anyString())).thenReturn(mode);
        when(access.storageFlushInterval(anyString())).thenReturn(60_000L);
        return new StorageFactory(config, access);
    }
}
