package io.github.hectorvent.floci.services.lambda.zip;

import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.core.common.AwsArnUtils;
import io.github.hectorvent.floci.core.common.AwsException;
import io.github.hectorvent.floci.core.common.FileTrees;
import io.github.hectorvent.floci.core.common.Resettable;
import io.github.hectorvent.floci.services.lambda.LambdaFunctionStore;
import io.github.hectorvent.floci.services.lambda.model.LambdaFunction;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * Manages on-disk locations of extracted Lambda function code.
 * Each function gets its own directory under
 * {@code <codePath>/<accountId>/@regions/<region>/}, mirroring the account- and
 * region-scoped Lambda namespace. The reserved namespace marker is intentional: it keeps
 * this layout disjoint from the preceding account/name layout, including when a legacy
 * function is named like a region.
 */
@ApplicationScoped
public class CodeStore implements Resettable {

    private static final Logger LOG = Logger.getLogger(CodeStore.class);

    /**
     * Separates a function's versions directory from any function's own code directory. Deliberately
     * outside {@link #sanitizeName}'s output character set, so the two namespaces cannot overlap.
     */
    private static final String VERSIONS_DIR_SUFFIX = "@versions";

    /** Namespace marker keeps the new layout disjoint from the preceding account/name layout. */
    private static final String REGIONS_DIR = "@regions";

    private final Path baseDir;
    private final LambdaFunctionStore functions;

    public CodeStore(EmulatorConfig config) {
        this(Path.of(config.services().lambda().codePath()));
    }

    @Inject
    public CodeStore(EmulatorConfig config, LambdaFunctionStore functions) {
        this.baseDir = Path.of(config.services().lambda().codePath());
        this.functions = functions;
    }

    public CodeStore(Path baseDir) {
        this.baseDir = baseDir;
        this.functions = null;
    }

    @Override
    public void clear() {
        deleteAccountDirectory(baseDir);
    }

    @Override
    public void checkAccountReset(String accountId) {
        if (functions == null) {
            return;
        }
        List<LambdaFunction> all = functions.listAll();
        List<Path> candidates = new ArrayList<>();
        candidates.add(baseDir.resolve(accountId).toAbsolutePath().normalize());
        for (LambdaFunction function : all) {
            if (accountId.equals(owner(function))) {
                candidates.add(getLegacyCodePath(function.getFunctionName()).toAbsolutePath().normalize());
            }
        }
        for (LambdaFunction function : all) {
            if (accountId.equals(owner(function))) {
                continue;
            }
            Path foreignRoot = baseDir.resolve(owner(function)).toAbsolutePath().normalize();
            if (Files.exists(foreignRoot) && candidates.stream().anyMatch(foreignRoot::startsWith)) {
                throw new AwsException("ConflictException", "Lambda code paths overlap account " + accountId
                        + "; migrate the foreign legacy code before resetting", 409);
            }
            if (function.getCodeLocalPath() == null) {
                continue;
            }
            Path foreignCode = Path.of(function.getCodeLocalPath()).toAbsolutePath().normalize();
            if (candidates.stream().anyMatch(foreignCode::startsWith)) {
                throw new AwsException("ConflictException", "Lambda code paths overlap account " + accountId
                        + "; migrate the foreign legacy code before resetting", 409);
            }
        }
    }

    private static String owner(LambdaFunction function) {
        return function.getAccountId() != null && !function.getAccountId().isBlank() ? function.getAccountId()
                : AwsArnUtils.accountOrDefault(function.getFunctionArn(), "");
    }

    @Override
    public void clearAccount(String accountId) {
        checkAccountReset(accountId);
        if (functions != null) {
            List<LambdaFunction> all = functions.listAll();
            for (LambdaFunction function : all) {
                if (!accountId.equals(owner(function))) {
                    continue;
                }
                Path legacy = getLegacyCodePath(function.getFunctionName());
                boolean shared = all.stream().anyMatch(other -> other != function
                        && !accountId.equals(owner(other))
                        && legacy.toString().equals(other.getCodeLocalPath()));
                if (!shared && !Files.isDirectory(legacy.resolve(REGIONS_DIR))) {
                    deleteAccountDirectory(legacy);
                }
            }
        }
        // This also removes the preceding account/function layout and every version's code.
        deleteAccountDirectory(baseDir.resolve(accountId));
    }

    private static void deleteAccountDirectory(Path directory) {
        try {
            FileTrees.delete(directory);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to purge Lambda code under " + directory, e);
        }
    }

    public Path getCodePath(String accountId, String region, String functionName) {
        return baseDir.resolve(sanitizeName(accountId))
                .resolve(REGIONS_DIR)
                .resolve(sanitizeName(region))
                .resolve(sanitizeName(functionName));
    }

    /**
     * The pre-account-scoped path a function's code would have extracted to before this class
     * added an account segment. Retained so {@link #delete} and code re-extraction can reclaim a
     * directory left behind by a function created before that migration.
     */
    public Path getLegacyCodePath(String functionName) {
        return baseDir.resolve(sanitizeName(functionName));
    }

    /**
     * Where a function's published-version code lives: one directory, holding a subdirectory per
     * published version.
     *
     * <p>A sibling of the {@code $LATEST} directory rather than a child of it. Extraction replaces
     * {@link #getCodePath}'s directory wholesale, so anything nested inside would be deleted on the
     * next deploy, which is the very thing a version's copy exists to survive.
     *
     * <p>The {@link #VERSIONS_DIR_SUFFIX} is what keeps that sibling from colliding with a real
     * function. {@link #sanitizeName} maps every name into {@code [a-zA-Z0-9_.-]}, so no function
     * can produce a directory name containing {@code @}, whatever it is called. A plainer
     * {@code <name>.v<n>} sibling carried no such guarantee, because a dot is an accepted character
     * in a function name: {@code LambdaArnUtils} validates against {@code [a-zA-Z0-9-_.]+}, which is
     * wider than the live service (issue #3238). So a function genuinely named {@code foo.v1} owns
     * the exact directory {@code foo}'s version 1 would otherwise claim, and deleting either one
     * silently corrupts the other.
     */
    public Path getVersionsPath(String accountId, String region, String functionName) {
        return baseDir.resolve(sanitizeName(accountId))
                .resolve(REGIONS_DIR)
                .resolve(sanitizeName(region))
                .resolve(sanitizeName(functionName) + VERSIONS_DIR_SUFFIX);
    }

    /** Where one published version's own copy of the code lives. */
    public Path getVersionCodePath(String accountId, String region, String functionName, String version) {
        return getVersionsPath(accountId, region, functionName).resolve(sanitizeName(version));
    }

    /**
     * Copies a function's current code into its own directory for {@code version}, replacing any
     * directory already there. Returns null when there is nothing to copy, which is the case for
     * image-backed and hot-reload functions.
     */
    public Path copyForVersion(String accountId, String region, String functionName, String version, Path source)
            throws IOException {
        if (source == null || !Files.isDirectory(source)) {
            return null;
        }
        Path target = getVersionCodePath(accountId, region, functionName, version);
        deleteDirectory(target, functionName);
        try (Stream<Path> walk = Files.walk(source)) {
            for (Path from : walk.toList()) {
                Path to = target.resolve(source.relativize(from).toString());
                if (Files.isDirectory(from)) {
                    Files.createDirectories(to);
                } else {
                    Files.createDirectories(to.getParent());
                    Files.copy(from, to, StandardCopyOption.REPLACE_EXISTING);
                }
            }
        } catch (UncheckedIOException e) {
            deleteDirectory(target, functionName);
            throw e.getCause();
        } catch (IOException e) {
            deleteDirectory(target, functionName);
            throw e;
        }
        return target;
    }

    /** Removes every published version's code for a function. */
    public void deleteVersions(String accountId, String region, String functionName) {
        deleteDirectory(getVersionsPath(accountId, region, functionName), functionName);
    }

    /**
     * Removes one published version's code, leaving the function's other versions and
     * {@code $LATEST} in place. Deleting a single version otherwise left its package on disk for
     * as long as the data directory lived, so a repeated publish/delete cycle grew without bound.
     */
    public void deleteVersion(String accountId, String region, String functionName, String version) {
        deleteDirectory(getVersionCodePath(accountId, region, functionName, version), functionName);
    }

    public void delete(String accountId, String region, String functionName) {
        deleteDirectory(getCodePath(accountId, region, functionName), functionName);
        deleteVersions(accountId, region, functionName);
    }

    /**
     * Best-effort removal of a function's pre-account-scoped directory. Deliberately NOT called
     * automatically from {@link #delete}: the pre-account-scoped layout gave every account's
     * same-named function the exact same directory, so it is only safe to remove once the caller
     * (see {@code LambdaService}) has confirmed no other account's function still references it.
     */
    public void deleteLegacy(String functionName) {
        deleteDirectory(getLegacyCodePath(functionName), functionName);
    }

    private void deleteDirectory(Path path, String functionName) {
        if (!Files.exists(path)) {
            return;
        }
        try {
            FileTrees.delete(path);
            LOG.debugv("Deleted code for function: {0}", functionName);
        } catch (IOException e) {
            LOG.warnv("Failed to delete code directory for {0}: {1}", functionName, e.getMessage());
        }
    }

    public boolean exists(String accountId, String region, String functionName) {
        Path codePath = getCodePath(accountId, region, functionName);
        if (!Files.exists(codePath)) {
            return false;
        }
        try (Stream<Path> listing = Files.list(codePath)) {
            return listing.findAny().isPresent();
        } catch (IOException e) {
            return false;
        }
    }

    /**
     * Replaces disallowed characters, then collapses any segment that consists entirely of dots
     * ({@code "."}, {@code ".."}, ...) to a safe placeholder: dots alone survive the character
     * replacement above but are special path segments that {@link Path#resolve} would otherwise
     * follow outside {@link #baseDir}.
     */
    private String sanitizeName(String name) {
        String sanitized = name.replaceAll("[^a-zA-Z0-9_\\-.]", "_");
        if (sanitized.isEmpty() || sanitized.chars().allMatch(c -> c == '.')) {
            return "_";
        }
        return sanitized;
    }
}
