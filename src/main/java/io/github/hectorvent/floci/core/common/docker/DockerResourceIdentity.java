package io.github.hectorvent.floci.core.common.docker;

import io.github.hectorvent.floci.config.EmulatorConfig;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.UUID;

/** Stable ownership identity for child Docker resources managed by one Floci instance. */
@ApplicationScoped
public class DockerResourceIdentity {

    private static final Logger LOG = Logger.getLogger(DockerResourceIdentity.class);
    private static final String ID_FILE = ".floci-docker-instance-id";

    private final EmulatorConfig config;
    private volatile String resolved;

    @Inject
    public DockerResourceIdentity(EmulatorConfig config) {
        this.config = config;
    }

    public String instanceId() {
        String current = resolved;
        if (current != null) {
            return current;
        }
        synchronized (this) {
            if (resolved == null) {
                resolved = resolve();
            }
            return resolved;
        }
    }

    private String resolve() {
        String namespace = ContainerStorageHelper.resourceNamespace(config);
        if (!namespace.isBlank()) {
            return "namespace:" + namespace;
        }

        Path identityFile;
        try {
            identityFile = Path.of(config.storage().persistentPath()).resolve(ID_FILE);
        } catch (RuntimeException e) {
            return ephemeralIdentity("the configured persistent path is invalid", e);
        }

        try {
            Files.createDirectories(identityFile.getParent());
            if (Files.isRegularFile(identityFile)) {
                String existing = Files.readString(identityFile).trim();
                if (!existing.isBlank()) {
                    return existing;
                }
            }

            String generated = UUID.randomUUID().toString();
            try {
                Files.writeString(identityFile, generated + System.lineSeparator(),
                        StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
                return generated;
            } catch (FileAlreadyExistsException race) {
                String existing = Files.readString(identityFile).trim();
                if (!existing.isBlank()) {
                    return existing;
                }
                throw race;
            }
        } catch (IOException | RuntimeException e) {
            return ephemeralIdentity("could not persist identity at " + identityFile, e);
        }
    }

    private static String ephemeralIdentity(String reason, Exception cause) {
        String identity = "ephemeral:" + UUID.randomUUID();
        LOG.warnv("Using non-recoverable Docker resource identity {0} because {1}: {2}",
                identity, reason, cause.getMessage());
        return identity;
    }
}
