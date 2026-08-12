package com.floci.test;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.DeleteTopicRequest;
import software.amazon.awssdk.services.sns.model.GetDataProtectionPolicyRequest;
import software.amazon.awssdk.services.sns.model.InvalidParameterException;
import software.amazon.awssdk.services.sns.model.NotFoundException;
import software.amazon.awssdk.services.sns.model.PutDataProtectionPolicyRequest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("SNS data protection policy management")
class SnsDataProtectionPolicyTest {

    private static final String POLICY =
            "{\"Name\":\"sdk-policy\",\"Version\":\"2021-06-01\",\"Statement\":[]}";

    private static SnsClient sns;
    private static String topicArn;

    @BeforeAll
    static void setUp() {
        sns = TestFixtures.snsClient();
        topicArn = sns.createTopic(request -> request.name("sdk-dpp-" + System.currentTimeMillis()))
                .topicArn();
    }

    @AfterAll
    static void tearDown() {
        if (sns == null) {
            return;
        }
        if (topicArn != null) {
            sns.deleteTopic(DeleteTopicRequest.builder().topicArn(topicArn).build());
        }
        sns.close();
    }

    @Test
    void managesPolicyLifecycleAndMapsServiceErrors() {
        var getRequest = GetDataProtectionPolicyRequest.builder().resourceArn(topicArn).build();
        assertThat(sns.getDataProtectionPolicy(getRequest).dataProtectionPolicy()).isNull();

        sns.putDataProtectionPolicy(PutDataProtectionPolicyRequest.builder()
                .resourceArn(topicArn)
                .dataProtectionPolicy(POLICY)
                .build());
        assertThat(sns.getDataProtectionPolicy(getRequest).dataProtectionPolicy()).isEqualTo(POLICY);

        sns.putDataProtectionPolicy(PutDataProtectionPolicyRequest.builder()
                .resourceArn(topicArn)
                .dataProtectionPolicy("")
                .build());
        assertThat(sns.getDataProtectionPolicy(getRequest).dataProtectionPolicy()).isNull();

        String missingArn = "arn:aws:sns:us-east-1:000000000000:sdk-dpp-missing";
        assertThatThrownBy(() -> sns.getDataProtectionPolicy(request -> request.resourceArn(missingArn)))
                .isInstanceOfSatisfying(NotFoundException.class, error -> {
                    assertThat(error.statusCode()).isEqualTo(404);
                    assertThat(error.awsErrorDetails().errorCode()).isEqualTo("NotFound");
                });

        String oversizedPolicy = "{\"p\":\"" + "a".repeat(30_713) + "\"}";
        assertThatThrownBy(() -> sns.putDataProtectionPolicy(request -> request
                .resourceArn(topicArn)
                .dataProtectionPolicy(oversizedPolicy)))
                .isInstanceOfSatisfying(InvalidParameterException.class, error -> {
                    assertThat(error.statusCode()).isEqualTo(400);
                    assertThat(error.awsErrorDetails().errorCode()).isEqualTo("InvalidParameter");
                });
    }
}
