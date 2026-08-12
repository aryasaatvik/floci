package com.floci.test;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketEncryptionRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.GetBucketEncryptionRequest;
import software.amazon.awssdk.services.s3.model.PutBucketEncryptionRequest;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;
import software.amazon.awssdk.services.s3.model.ServerSideEncryptionByDefault;
import software.amazon.awssdk.services.s3.model.ServerSideEncryptionConfiguration;
import software.amazon.awssdk.services.s3.model.ServerSideEncryptionRule;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("S3 bucket encryption")
class S3BucketEncryptionTest {

    private static final String KMS_KEY = "arn:aws:kms:us-east-1:000000000000:key/sdk-test";

    @Test
    @DisplayName("bucket encryption round-trips through the AWS SDK and delete restores SSE-S3")
    void bucketEncryptionLifecycle() {
        String bucket = TestFixtures.uniqueName("sdk-s3-encryption");

        try (S3Client s3 = TestFixtures.s3Client()) {
            s3.createBucket(CreateBucketRequest.builder().bucket(bucket).build());
            try {
                var initial = s3.getBucketEncryption(
                        GetBucketEncryptionRequest.builder().bucket(bucket).build());
                assertEncryptionRule(
                        initial.serverSideEncryptionConfiguration(),
                        ServerSideEncryption.AES256,
                        null,
                        false);

                var configuration = ServerSideEncryptionConfiguration.builder()
                        .rules(ServerSideEncryptionRule.builder()
                                .applyServerSideEncryptionByDefault(ServerSideEncryptionByDefault.builder()
                                        .sseAlgorithm(ServerSideEncryption.AWS_KMS)
                                        .kmsMasterKeyID(KMS_KEY)
                                        .build())
                                .bucketKeyEnabled(true)
                                .build())
                        .build();
                s3.putBucketEncryption(PutBucketEncryptionRequest.builder()
                        .bucket(bucket)
                        .serverSideEncryptionConfiguration(configuration)
                        .build());

                var persisted = s3.getBucketEncryption(
                        GetBucketEncryptionRequest.builder().bucket(bucket).build());
                assertEncryptionRule(
                        persisted.serverSideEncryptionConfiguration(),
                        ServerSideEncryption.AWS_KMS,
                        KMS_KEY,
                        true);

                s3.deleteBucketEncryption(
                        DeleteBucketEncryptionRequest.builder().bucket(bucket).build());

                var restored = s3.getBucketEncryption(
                        GetBucketEncryptionRequest.builder().bucket(bucket).build());
                assertEncryptionRule(
                        restored.serverSideEncryptionConfiguration(),
                        ServerSideEncryption.AES256,
                        null,
                        false);
            } finally {
                s3.deleteBucket(DeleteBucketRequest.builder().bucket(bucket).build());
            }
        }
    }

    private static void assertEncryptionRule(
            ServerSideEncryptionConfiguration configuration,
            ServerSideEncryption algorithm,
            String kmsKey,
            boolean bucketKeyEnabled) {
        assertThat(configuration).isNotNull();
        assertThat(configuration.rules()).singleElement().satisfies(rule -> {
            assertThat(rule.applyServerSideEncryptionByDefault()).isNotNull();
            assertThat(rule.applyServerSideEncryptionByDefault().sseAlgorithm()).isEqualTo(algorithm);
            assertThat(rule.applyServerSideEncryptionByDefault().kmsMasterKeyID()).isEqualTo(kmsKey);
            assertThat(rule.bucketKeyEnabled()).isEqualTo(bucketKeyEnabled);
        });
    }
}
