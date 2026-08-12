package io.github.hectorvent.floci.core.storage;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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
    void scanAllAccountEntriesPreservesOwnersAndTreatsNonAwsPrefixesAsLegacy() {
        InMemoryStorage<String, String> raw = new InMemoryStorage<>();
        raw.put("123456789012/us-east-1::api-a", "account-a");
        raw.put("210987654321/us-east-1::api-b", "account-b");
        raw.put("legacy/path", "legacy");
        raw.put("12345678901/short", "short-prefix");
        raw.put("1234567890123/long", "long-prefix");
        raw.put("１２３４５６７８９０１２/unicode", "unicode-prefix");

        AccountAwareStorageBackend<String> storage =
                new AccountAwareStorageBackend<>(raw, null, "000000000000");

        List<AccountAwareStorageBackend.AccountEntry<String>> entries =
                storage.scanAllAccountEntries(key -> true);

        assertEquals(Set.of(
                new AccountAwareStorageBackend.AccountEntry<>("123456789012", "us-east-1::api-a", "account-a"),
                new AccountAwareStorageBackend.AccountEntry<>("210987654321", "us-east-1::api-b", "account-b"),
                new AccountAwareStorageBackend.AccountEntry<>("000000000000", "legacy/path", "legacy"),
                new AccountAwareStorageBackend.AccountEntry<>("000000000000", "12345678901/short", "short-prefix"),
                new AccountAwareStorageBackend.AccountEntry<>("000000000000", "1234567890123/long", "long-prefix"),
                new AccountAwareStorageBackend.AccountEntry<>(
                        "000000000000", "１２３４５６７８９０１２/unicode", "unicode-prefix")),
                Set.copyOf(entries));
    }

    @Test
    void scanAllAccountEntriesFiltersAccountRelativeKeys() {
        InMemoryStorage<String, String> raw = new InMemoryStorage<>();
        raw.put("123456789012/us-east-1::api-a", "account-a");
        raw.put("210987654321/eu-west-1::api-b", "account-b");
        raw.put("us-east-1::legacy", "legacy");

        AccountAwareStorageBackend<String> storage =
                new AccountAwareStorageBackend<>(raw, null, "000000000000");

        assertEquals(Set.of(
                new AccountAwareStorageBackend.AccountEntry<>("123456789012", "us-east-1::api-a", "account-a"),
                new AccountAwareStorageBackend.AccountEntry<>("000000000000", "us-east-1::legacy", "legacy")),
                Set.copyOf(storage.scanAllAccountEntries(key -> key.startsWith("us-east-1::"))));
    }
}
