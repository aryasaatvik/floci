package io.github.hectorvent.floci.services.s3;

import io.github.hectorvent.floci.core.storage.AccountAwareStorageBackend;
import io.github.hectorvent.floci.services.s3.model.Bucket;
import io.github.hectorvent.floci.services.s3.model.MultipartUpload;
import io.github.hectorvent.floci.services.s3.model.S3Object;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class S3AccountResetTest {
    @TempDir Path directory;
    private static final String ACCOUNT = "111111111111";
    private static final String OTHER = "222222222222";

    @Test
    void scopedResetDeletesPayloadsAndMultipartFilesWhilePreservingOtherAccount() throws Exception {
        AccountAwareStorageBackend<Bucket> buckets = AccountAwareStorageBackend.inMemory(ACCOUNT);
        AccountAwareStorageBackend<S3Object> objects = AccountAwareStorageBackend.inMemory(ACCOUNT);
        S3Service service = new S3Service(buckets, objects, directory, false);
        service.createBucket("target-bucket", "us-east-1");
        buckets.putForAccount(OTHER, "other-bucket", new Bucket("other-bucket"));
        Path target = directory.resolve(".accounts").resolve(ACCOUNT).resolve("target-bucket/payload.s3data");
        Path other = directory.resolve(".accounts").resolve(OTHER).resolve("other-bucket/payload.s3data");
        Files.createDirectories(target.getParent());
        Files.createDirectories(other.getParent());
        Files.writeString(target, "target");
        Files.writeString(other, "other");
        MultipartUpload upload = service.initiateMultipartUpload("target-bucket", "key", "text/plain", Map.of(), null);
        service.uploadPart("target-bucket", "key", upload.getUploadId(), 1, new byte[]{1, 2, 3});
        Path parts = directory.resolve(".multipart").resolve(upload.getUploadId());
        assertTrue(Files.exists(parts));
        service.clearAccount(ACCOUNT);
        assertFalse(Files.exists(target));
        assertFalse(Files.exists(parts));
        assertEquals("other", Files.readString(other));
        assertTrue(buckets.getForAccount(OTHER, "other-bucket").isPresent());
    }
}
