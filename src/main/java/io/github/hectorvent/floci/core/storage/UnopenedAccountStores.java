package io.github.hectorvent.floci.core.storage;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Plans changes to dormant stores without creating, migrating, or repairing a backend. */
final class UnopenedAccountStores {

    private static final Pattern SHARD_NAME = Pattern.compile(".*-[0-9a-f]{16}(?:\\.[0-9]+-of-[0-9]+)?\\.json");
    private static final TypeReference<Map<String, JsonNode>> RAW_STORE = new TypeReference<>() {};
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

    private UnopenedAccountStores() {}

    record Purge(List<Path> registeredPaths, Write write) {
        @FunctionalInterface
        interface Write {
            void run() throws IOException;
        }
    }

    static List<Purge> plan(Path root, Set<Path> registeredPaths, String accountId, String defaultAccount)
            throws IOException {
        if (!Files.isDirectory(root)) {
            return List.of();
        }
        Predicate<String> ownsKey = key -> AccountAwareStorageBackend.ownsKey(key, accountId, defaultAccount);
        List<Path> entries;
        try (Stream<Path> paths = Files.list(root)) {
            entries = paths.toList();
        }
        List<Purge> purges = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        for (Path entry : entries) {
            String name = entry.getFileName().toString();
            if (Files.isDirectory(entry, LinkOption.NOFOLLOW_LINKS)) {
                if (!registeredPaths.contains(entry)) {
                    planPartitioned(entry, ownsKey, purges);
                }
            } else if (name.endsWith(".json") || name.endsWith(".wal")) {
                String stem = name.replaceFirst("(?:-snapshot)?\\.(?:json|wal)$", "");
                if (seen.add(stem)) {
                    FlatStore files = new FlatStore(root, stem);
                    if (files.registeredPaths().stream().noneMatch(registeredPaths::contains)) {
                        planFlat(files, ownsKey, purges);
                    }
                }
            }
        }
        return purges;
    }

    static <V> Map<String, V> readFlat(Path root, String fileName, TypeReference<Map<String, V>> type) throws IOException {
        FlatStore files = new FlatStore(root, fileName.substring(0, fileName.length() - ".json".length()));
        return files.usesWal()
                ? WalStorage.inspectForReclaim(files.liveSnapshot(), files.journal(), type).snapshot()
                : readPlain(files.plain(), type);
    }

    private static void planFlat(FlatStore files, Predicate<String> ownsKey, List<Purge> purges) throws IOException {
        if (files.usesWal()) {
            WalStorage<String, JsonNode> store = WalStorage.inspectForReclaim(files.liveSnapshot(), files.journal(), RAW_STORE);
            Set<String> keys = store.keys().stream().filter(ownsKey).collect(Collectors.toSet());
            if (!keys.isEmpty()) {
                purges.add(new Purge(files.registeredPaths(), () -> store.reclaimUnopenedKeys(keys)));
            }
        } else {
            Map<String, JsonNode> contents = new HashMap<>(readPlain(files.plain(), RAW_STORE));
            if (contents.keySet().removeIf(ownsKey)) {
                purges.add(new Purge(files.registeredPaths(),
                        () -> StorageFiles.writeAtomically(files.plain(), temp -> MAPPER.writeValue(temp.toFile(), contents))));
            }
        }
    }

    private static <V> Map<String, V> readPlain(Path plain, TypeReference<Map<String, V>> type) throws IOException {
        return StorageFiles.isRegularFileIfPresent(plain) ? MAPPER.readValue(plain.toFile(), type) : Map.of();
    }

    private static void planPartitioned(Path directory, Predicate<String> ownsKey, List<Purge> purges) throws IOException {
        Path journalPath = directory.resolve(PartitionedFileStorage.JOURNAL_FILE);
        boolean recognized = StorageFiles.isRegularFileIfPresent(journalPath);
        List<Path> targetShards = new ArrayList<>();
        boolean malformed = false;
        try (Stream<Path> files = Files.list(directory)) {
            for (Path file : files.filter(path -> path.getFileName().toString().endsWith(PartitionedFileStorage.FILE_SUFFIX)
                    && !Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)).toList()) {
                if (!StorageFiles.isRegularFileIfPresent(file)) {
                    continue;
                }
                JsonNode contents = MAPPER.readTree(file.toFile());
                JsonNode key = contents.path(PartitionedFileStorage.KEY_FIELD);
                JsonNode entries = contents.path(PartitionedFileStorage.ENTRIES_FIELD);
                recognized |= !key.isMissingNode() || !entries.isMissingNode()
                        || SHARD_NAME.matcher(file.getFileName().toString()).matches();
                if (!key.isTextual() || key.asText().isEmpty() || !entries.isObject()
                        || entries.valueStream().anyMatch(JsonNode::isNull)) {
                    malformed = true;
                } else if (ownsKey.test(key.asText())) {
                    targetShards.add(file);
                }
            }
        }
        if (recognized && malformed) {
            throw new IOException("Unreadable partitioned storage shard");
        }

        EntryJournal<JsonNode> journal = new EntryJournal<>(journalPath, MAPPER.constructType(JsonNode.class),
                MAPPER.getTypeFactory().constructMapType(Map.class, String.class, JsonNode.class));
        Set<String> journalKeys = new HashSet<>();
        // Add a tombstone only for a key the journal could still recreate after its shards go away.
        journal.inspect(change -> {
            if (ownsKey.test(change.key())) {
                switch (change) {
                    case EntryJournal.Put<JsonNode> put -> journalKeys.add(put.key());
                    case EntryJournal.PutEntry<JsonNode> put -> journalKeys.add(put.key());
                    case EntryJournal.Delete<JsonNode> delete -> journalKeys.remove(delete.key());
                    case EntryJournal.RemoveEntry<JsonNode> ignored -> {
                        // Removing an entry cannot recreate its parent key.
                    }
                }
            }
        });
        if (!targetShards.isEmpty() || !journalKeys.isEmpty()) {
            purges.add(new Purge(List.of(directory), () -> {
                if (!journalKeys.isEmpty()) {
                    journal.open();
                    try {
                        for (String key : journalKeys) {
                            if (!journal.delete(key)) {
                                throw new IOException("Failed to purge account key from a partitioned journal");
                            }
                        }
                    } finally {
                        journal.close();
                    }
                }
                for (Path shard : targetShards) {
                    Files.deleteIfExists(shard);
                }
            }));
        }
    }

    private record FlatStore(Path plain, Path snapshot, Path journal, Path partitioned) {
        private FlatStore(Path root, String stem) {
            this(root.resolve(stem + ".json"), root.resolve(stem + "-snapshot.json"),
                    root.resolve(stem + ".wal"), root.resolve(stem));
        }

        List<Path> registeredPaths() {
            return List.of(plain, partitioned);
        }

        boolean usesWal() throws IOException {
            boolean snapshotExists = StorageFiles.isRegularFileIfPresent(snapshot);
            boolean journalExists = StorageFiles.isRegularFileIfPresent(journal);
            if (snapshotExists && StorageFiles.isRegularFileIfPresent(plain)) {
                long auxiliaryTime = Math.max(Files.getLastModifiedTime(snapshot).toMillis(),
                        journalExists ? Files.getLastModifiedTime(journal).toMillis() : Long.MIN_VALUE);
                return Files.getLastModifiedTime(plain).toMillis() <= auxiliaryTime;
            }
            return snapshotExists || journalExists;
        }

        Path liveSnapshot() throws IOException {
            return StorageFiles.isRegularFileIfPresent(snapshot) ? snapshot : plain;
        }
    }
}
