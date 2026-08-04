package com.floci.test;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
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

    private static final String BUCKET = TestFixtures.uniqueName("sdk-s3-encryption");
    private static final String KMS_KEY = "arn:aws:kms:us-east-1:000000000000:key/sdk-test";
    private static S3Client s3;

    @BeforeAll
    static void setup() {
        s3 = TestFixtures.s3Client();
        s3.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build());
    }

    @AfterAll
    static void cleanup() {
        if (s3 == null) return;
        try {
            s3.deleteBucket(DeleteBucketRequest.builder().bucket(BUCKET).build());
        } finally {
            s3.close();
        }
    }

    @Test
    @DisplayName("GetBucketEncryption returns the SSE-S3 default for persisted buckets without an explicit rule")
    void getBucketEncryptionFallsBackToSseS3() {
        var response = s3.getBucketEncryption(GetBucketEncryptionRequest.builder().bucket(BUCKET).build());

        assertThat(response.serverSideEncryptionConfiguration().rules()).singleElement().satisfies(rule -> {
            assertThat(rule.applyServerSideEncryptionByDefault().sseAlgorithm())
                    .isEqualTo(ServerSideEncryption.AES256);
            assertThat(rule.bucketKeyEnabled()).isFalse();
        });
    }

    @Test
    @DisplayName("PutBucketEncryption persists an AWS SDK configuration and DeleteBucketEncryption restores the default")
    void putAndDeleteBucketEncryptionRoundTrip() {
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
                .bucket(BUCKET)
                .serverSideEncryptionConfiguration(configuration)
                .build());

        var persisted = s3.getBucketEncryption(GetBucketEncryptionRequest.builder().bucket(BUCKET).build())
                .serverSideEncryptionConfiguration().rules().getFirst();
        assertThat(persisted.applyServerSideEncryptionByDefault().sseAlgorithm())
                .isEqualTo(ServerSideEncryption.AWS_KMS);
        assertThat(persisted.applyServerSideEncryptionByDefault().kmsMasterKeyID()).isEqualTo(KMS_KEY);
        assertThat(persisted.bucketKeyEnabled()).isTrue();

        s3.deleteBucketEncryption(DeleteBucketEncryptionRequest.builder().bucket(BUCKET).build());

        var fallback = s3.getBucketEncryption(GetBucketEncryptionRequest.builder().bucket(BUCKET).build())
                .serverSideEncryptionConfiguration().rules().getFirst();
        assertThat(fallback.applyServerSideEncryptionByDefault().sseAlgorithm())
                .isEqualTo(ServerSideEncryption.AES256);
        assertThat(fallback.bucketKeyEnabled()).isFalse();
    }
}
