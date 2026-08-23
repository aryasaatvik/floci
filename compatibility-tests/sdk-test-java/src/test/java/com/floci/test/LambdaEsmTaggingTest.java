package com.floci.test;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.CreateEventSourceMappingRequest;
import software.amazon.awssdk.services.lambda.model.CreateEventSourceMappingResponse;
import software.amazon.awssdk.services.lambda.model.CreateFunctionRequest;
import software.amazon.awssdk.services.lambda.model.DeleteEventSourceMappingRequest;
import software.amazon.awssdk.services.lambda.model.DeleteFunctionRequest;
import software.amazon.awssdk.services.lambda.model.FunctionCode;
import software.amazon.awssdk.services.lambda.model.GetEventSourceMappingRequest;
import software.amazon.awssdk.services.lambda.model.ListEventSourceMappingsRequest;
import software.amazon.awssdk.services.lambda.model.ListTagsRequest;
import software.amazon.awssdk.services.lambda.model.ResourceNotFoundException;
import software.amazon.awssdk.services.lambda.model.Runtime;
import software.amazon.awssdk.services.lambda.model.TagResourceRequest;
import software.amazon.awssdk.services.lambda.model.UntagResourceRequest;
import software.amazon.awssdk.services.lambda.model.UpdateEventSourceMappingRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.DeleteQueueRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("Lambda - ESM tagging")
class LambdaEsmTaggingTest {

    private static final String FUNCTION_NAME = "sdk-test-esm-tagging-fn";
    private static final String QUEUE_NAME = "sdk-test-esm-tagging-queue";
    private static final String ROLE = "arn:aws:iam::000000000000:role/lambda-role";
    private static final String FUNCTION_ARN =
            "arn:aws:lambda:us-east-1:000000000000:function:" + FUNCTION_NAME;
    private static final String MAPPING_ARN_PREFIX =
            "arn:aws:lambda:us-east-1:000000000000:event-source-mapping:";

    private static LambdaClient lambda;
    private static SqsClient sqs;
    private static String queueUrl;
    private static String mappingUuid;

    @BeforeAll
    static void setup() {
        lambda = TestFixtures.lambdaClient();
        sqs = TestFixtures.sqsClient();

        lambda.createFunction(CreateFunctionRequest.builder()
                .functionName(FUNCTION_NAME)
                .runtime(Runtime.NODEJS20_X)
                .role(ROLE)
                .handler("index.handler")
                .code(FunctionCode.builder()
                        .zipFile(SdkBytes.fromByteArray(LambdaUtils.minimalZip()))
                        .build())
                .build());

        queueUrl = sqs.createQueue(CreateQueueRequest.builder()
                .queueName(QUEUE_NAME)
                .build())
                .queueUrl();
    }

    @AfterAll
    static void cleanup() {
        if (lambda != null) {
            if (mappingUuid != null) {
                try {
                    lambda.deleteEventSourceMapping(DeleteEventSourceMappingRequest.builder()
                            .uuid(mappingUuid)
                            .build());
                } catch (Exception error) {
                    System.err.println("Failed to clean up Lambda ESM " + mappingUuid + ": " + error.getMessage());
                }
            }
            try {
                lambda.deleteFunction(DeleteFunctionRequest.builder()
                        .functionName(FUNCTION_NAME)
                        .build());
            } catch (Exception error) {
                System.err.println("Failed to clean up Lambda function " + FUNCTION_NAME + ": " + error.getMessage());
            }
            lambda.close();
        }
        if (sqs != null) {
            try {
                sqs.deleteQueue(DeleteQueueRequest.builder().queueUrl(queueUrl).build());
            } catch (Exception error) {
                System.err.println("Failed to clean up SQS queue " + queueUrl + ": " + error.getMessage());
            }
            sqs.close();
        }
    }

    @Test
    void mappingIdentityAndTagsRoundTripThroughAwsSdk() {
        String queueArn = sqs.getQueueAttributes(GetQueueAttributesRequest.builder()
                .queueUrl(queueUrl)
                .attributeNames(QueueAttributeName.QUEUE_ARN)
                .build())
                .attributes()
                .get(QueueAttributeName.QUEUE_ARN);

        CreateEventSourceMappingResponse created = lambda.createEventSourceMapping(
                CreateEventSourceMappingRequest.builder()
                        .functionName(FUNCTION_NAME)
                        .eventSourceArn(queueArn)
                        .tags(Map.of("team", "platform", "env", "dev"))
                        .build());
        mappingUuid = created.uuid();

        assertIdentity(created.uuid(), created.functionArn(), created.eventSourceMappingArn());

        var fetched = lambda.getEventSourceMapping(
                GetEventSourceMappingRequest.builder().uuid(mappingUuid).build());
        assertIdentity(fetched.uuid(), fetched.functionArn(), fetched.eventSourceMappingArn());

        var listed = lambda.listEventSourceMappings(ListEventSourceMappingsRequest.builder()
                        .functionName(FUNCTION_ARN)
                        .eventSourceArn(queueArn)
                        .build())
                .eventSourceMappings()
                .stream()
                .filter(mapping -> mappingUuid.equals(mapping.uuid()))
                .findFirst()
                .orElseThrow();
        assertIdentity(listed.uuid(), listed.functionArn(), listed.eventSourceMappingArn());

        var updated = lambda.updateEventSourceMapping(UpdateEventSourceMappingRequest.builder()
                .uuid(mappingUuid)
                .batchSize(8)
                .build());
        assertIdentity(updated.uuid(), updated.functionArn(), updated.eventSourceMappingArn());
        assertThat(updated.batchSize()).isEqualTo(8);

        assertThat(lambda.listTags(ListTagsRequest.builder()
                        .resource(created.eventSourceMappingArn())
                        .build())
                .tags()).containsExactlyInAnyOrderEntriesOf(Map.of("team", "platform", "env", "dev"));

        lambda.tagResource(TagResourceRequest.builder()
                .resource(created.eventSourceMappingArn())
                .tags(Map.of("env", "prod", "owner", "platform"))
                .build());
        lambda.untagResource(UntagResourceRequest.builder()
                .resource(created.eventSourceMappingArn())
                .tagKeys("team", "owner")
                .build());

        assertThat(lambda.listTags(ListTagsRequest.builder()
                        .resource(created.eventSourceMappingArn())
                        .build())
                .tags()).containsExactly(Map.entry("env", "prod"));

        String mismatchedArn = created.eventSourceMappingArn()
                .replace(":000000000000:", ":000000000001:");
        assertThatThrownBy(() -> lambda.listTags(ListTagsRequest.builder()
                .resource(mismatchedArn)
                .build()))
                .isInstanceOf(ResourceNotFoundException.class);
    }

    private static void assertIdentity(String uuid, String functionArn, String eventSourceMappingArn) {
        assertThat(uuid).isEqualTo(mappingUuid);
        assertThat(functionArn)
                .isEqualTo(FUNCTION_ARN)
                .contains(":function:")
                .doesNotContain(":event-source-mapping:");
        assertThat(eventSourceMappingArn)
                .isEqualTo(MAPPING_ARN_PREFIX + mappingUuid)
                .contains(":event-source-mapping:")
                .doesNotContain(":function:");
    }
}
