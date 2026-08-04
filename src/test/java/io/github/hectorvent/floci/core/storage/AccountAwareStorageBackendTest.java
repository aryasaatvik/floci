package io.github.hectorvent.floci.core.storage;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AccountAwareStorageBackendTest {

    @Test
    void scanAllAccountsRawAttributesLegacyUnprefixedKeysToDefaultAccount() {
        InMemoryStorage<String, String> raw = new InMemoryStorage<>();
        // Simulates data persisted before multi-account support existed: no account
        // segment at all, unlike every key AccountAwareStorageBackend itself writes.
        raw.put("us-east-1::LegacyTable", "legacy-value");
        raw.put("111111111111/us-east-1::NewTable", "new-value");

        AccountAwareStorageBackend<String> aware = new AccountAwareStorageBackend<>(raw, null, "000000000000");

        Map<String, String> result = aware.scanAllAccountsRaw();

        assertEquals(2, result.size());
        assertEquals("legacy-value", result.get("000000000000/us-east-1::LegacyTable"),
                "a pre-multi-account key must be attributed to the default account, not dropped");
        assertEquals("new-value", result.get("111111111111/us-east-1::NewTable"));
        assertTrue(result.keySet().stream().allMatch(k -> k.indexOf('/') >= 0),
                "every returned key must carry an account segment");
    }

    @Test
    void scanAllAccountsRawMigratesLegacyKeyIntoUnderlyingStorage() {
        InMemoryStorage<String, String> raw = new InMemoryStorage<>();
        raw.put("us-east-1::LegacyTable", "legacy-value");

        AccountAwareStorageBackend<String> aware = new AccountAwareStorageBackend<>(raw, null, "000000000000");
        aware.scanAllAccountsRaw();

        assertEquals(Optional.empty(), raw.get("us-east-1::LegacyTable"),
                "the bare legacy key must not be left sitting in storage after being migrated");
        assertEquals(Optional.of("legacy-value"), raw.get("000000000000/us-east-1::LegacyTable"));
    }

    @Test
    void scanAllAccountsRawPrefersAlreadyPrefixedEntryOverStaleLegacyKeyAndDeletesTheStaleOne() {
        InMemoryStorage<String, String> raw = new InMemoryStorage<>();
        // A legacy key already superseded by a real write under its proper prefix.
        raw.put("us-east-1::Orders", "stale-legacy-value");
        raw.put("000000000000/us-east-1::Orders", "current-value");

        AccountAwareStorageBackend<String> aware = new AccountAwareStorageBackend<>(raw, null, "000000000000");
        Map<String, String> result = aware.scanAllAccountsRaw();

        assertEquals(1, result.size());
        assertEquals("current-value", result.get("000000000000/us-east-1::Orders"),
                "the already-prefixed, current entry must win over a stale legacy duplicate");
        assertEquals(Optional.empty(), raw.get("us-east-1::Orders"),
                "the superseded legacy key must be deleted, not left to collide again later");
    }

    @Test
    void scanAllAccountEntriesPreservesOwnersAndLegacyKeys() {
        InMemoryStorage<String, String> raw = new InMemoryStorage<>();
        raw.put("123456789012/us-east-1::api-a", "account-a");
        raw.put("210987654321/us-east-1::api-b", "account-b");
        raw.put("legacy/path", "legacy");

        AccountAwareStorageBackend<String> storage =
                new AccountAwareStorageBackend<>(raw, null, "000000000000");

        Map<String, AccountAwareStorageBackend.AccountEntry<String>> entries = storage
                .scanAllAccountEntries(key -> true)
                .stream()
                .collect(Collectors.toMap(
                        AccountAwareStorageBackend.AccountEntry::value,
                        Function.identity()));

        assertEquals("123456789012", entries.get("account-a").accountId());
        assertEquals("us-east-1::api-a", entries.get("account-a").key());
        assertEquals("210987654321", entries.get("account-b").accountId());
        assertEquals("us-east-1::api-b", entries.get("account-b").key());
        assertEquals("000000000000", entries.get("legacy").accountId());
        assertEquals("legacy/path", entries.get("legacy").key());
    }
}
