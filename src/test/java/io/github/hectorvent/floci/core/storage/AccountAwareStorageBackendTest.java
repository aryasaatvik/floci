package io.github.hectorvent.floci.core.storage;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AccountAwareStorageBackendTest {

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
