package io.github.hectorvent.floci.services.lambda.zip;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CodeStoreTest {

    @TempDir
    Path tempDir;

    @Test
    void isolatesImmutableCodePathsByAccountRegionFunctionAndDigest() throws Exception {
        CodeStore store = new CodeStore(tempDir);
        Path accountA = store.getCodePath(
                "111111111111", "us-east-1", "shared-fn", "a".repeat(64));
        Path accountB = store.getCodePath(
                "222222222222", "us-east-1", "shared-fn", "b".repeat(64));
        Path otherRegion = store.getCodePath(
                "111111111111", "eu-west-1", "shared-fn", "a".repeat(64));
        Path updatedCode = store.getCodePath(
                "111111111111", "us-east-1", "shared-fn", "c".repeat(64));

        assertNotEquals(accountA, accountB);
        assertNotEquals(accountA, otherRegion);
        assertNotEquals(accountA, updatedCode);

        Files.createDirectories(accountA);
        Files.writeString(accountA.resolve("handler.js"), "version-a");
        Files.createDirectories(accountB);
        Files.writeString(accountB.resolve("handler.js"), "version-b");

        assertTrue(store.exists("111111111111", "us-east-1", "shared-fn", "a".repeat(64)));
        assertTrue(store.exists("222222222222", "us-east-1", "shared-fn", "b".repeat(64)));

        store.delete("111111111111", "us-east-1", "shared-fn");

        assertFalse(Files.exists(accountA));
        assertTrue(Files.exists(accountB));
    }
}
