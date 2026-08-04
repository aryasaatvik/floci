package io.github.hectorvent.floci.services.lambda.launcher;

import io.github.hectorvent.floci.core.common.AwsArnUtils;
import io.github.hectorvent.floci.services.lambda.model.LambdaFunction;

/** Canonical identity of one Lambda execution-environment configuration. */
public record LambdaExecutionEnvironmentId(
        String accountId,
        String region,
        String functionName,
        String version,
        String revisionId) {

    private static final String DEFAULT_ACCOUNT = "000000000000";
    private static final String DEFAULT_REGION = "us-east-1";
    private static final String LATEST = "$LATEST";

    public static LambdaExecutionEnvironmentId from(LambdaFunction function) {
        String arn = function.getFunctionArn();
        String accountId = valueOrDefault(function.getAccountId(),
                AwsArnUtils.accountOrDefault(arn, DEFAULT_ACCOUNT));
        String region = AwsArnUtils.regionOrDefault(arn, DEFAULT_REGION);
        return new LambdaExecutionEnvironmentId(
                accountId,
                region,
                function.getFunctionName(),
                valueOrDefault(function.getVersion(), LATEST),
                valueOrDefault(function.getRevisionId(), ""));
    }

    public static LambdaExecutionEnvironmentId legacy(String functionName) {
        return new LambdaExecutionEnvironmentId(
                DEFAULT_ACCOUNT, DEFAULT_REGION, functionName, LATEST, "");
    }

    public String qualifiedFunctionArn() {
        String functionArn = "arn:aws:lambda:" + region + ":" + accountId
                + ":function:" + functionName;
        return LATEST.equals(version) ? functionArn : functionArn + ":" + version;
    }

    private static String valueOrDefault(String value, String fallback) {
        return value == null || value.isBlank() ? fallback : value;
    }
}
