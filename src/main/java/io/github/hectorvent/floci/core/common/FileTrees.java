package io.github.hectorvent.floci.core.common;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

public final class FileTrees {

    private FileTrees() {
    }

    /** Validates the account-owned path beneath a configured storage root without following links. */
    public static boolean checkScopedRoot(Path storageRoot, Path scopedRoot) throws IOException {
        Path root = storageRoot.toAbsolutePath().normalize();
        Path target = scopedRoot.toAbsolutePath().normalize();
        if (!target.startsWith(root) || target.equals(root)) {
            throw new IOException("Invalid account storage path");
        }
        Path current = root;
        for (Path segment : root.relativize(target)) {
            current = current.resolve(segment);
            try {
                BasicFileAttributes attributes = Files.readAttributes(
                        current, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
                if (!attributes.isDirectory()) {
                    throw new IOException("Account storage path is not a directory");
                }
            } catch (NoSuchFileException absent) {
                return false;
            }
        }
        return true;
    }

    /** Sizes regular files from metadata only. Symbolic links within the tree are neither followed nor counted. */
    public static long regularFileBytes(Path storageRoot, Path scopedRoot) throws IOException {
        if (!checkScopedRoot(storageRoot, scopedRoot)) {
            return 0;
        }
        final class Sizer extends SimpleFileVisitor<Path> {
            private long bytes;

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attributes) {
                if (attributes.isRegularFile()) {
                    bytes = Math.addExact(bytes, attributes.size());
                }
                return FileVisitResult.CONTINUE;
            }
        }
        Sizer sizer = new Sizer();
        Files.walkFileTree(scopedRoot, sizer);
        return sizer.bytes;
    }

    /**
     * Deletes a file, or a directory and everything under it. A missing path is not an error. A
     * failure does not stop the rest: the first one is thrown, with the later ones suppressed.
     */
    public static void delete(Path root) throws IOException {
        if (!Files.exists(root)) {
            return;
        }
        List<Path> paths;
        try (Stream<Path> walk = Files.walk(root)) {
            paths = walk.sorted(Comparator.reverseOrder()).toList();
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
        IOException failure = null;
        for (Path path : paths) {
            try {
                Files.deleteIfExists(path);
            } catch (IOException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }
}
