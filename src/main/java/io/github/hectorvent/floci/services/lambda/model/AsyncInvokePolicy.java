package io.github.hectorvent.floci.services.lambda.model;

/**
 * How an accepted asynchronous (Event) invocation is retried and where its outcome is recorded,
 * resolved from the function's event-invoke config when the event is accepted.
 *
 * @param invokedFunctionArn qualified ARN the caller invoked ({@code :$LATEST}, a version, or an alias)
 * @param maximumRetryAttempts retries after the first failed attempt
 * @param maximumEventAgeSeconds age after which a pending retry is dropped
 * @param onSuccessDestination destination ARN for a successful invocation record, or null
 * @param onFailureDestination destination ARN for a discarded event's record, or null
 */
public record AsyncInvokePolicy(String invokedFunctionArn,
                                int maximumRetryAttempts,
                                int maximumEventAgeSeconds,
                                String onSuccessDestination,
                                String onFailureDestination) {

    /** AWS applies these when a function has no event-invoke config. */
    public static final int DEFAULT_MAXIMUM_RETRY_ATTEMPTS = 2;
    public static final int DEFAULT_MAXIMUM_EVENT_AGE_SECONDS = 21600;

    public static AsyncInvokePolicy from(String invokedFunctionArn, FunctionEventInvokeConfig config) {
        if (config == null) {
            return new AsyncInvokePolicy(invokedFunctionArn,
                    DEFAULT_MAXIMUM_RETRY_ATTEMPTS, DEFAULT_MAXIMUM_EVENT_AGE_SECONDS, null, null);
        }
        int retries = config.getMaximumRetryAttempts() != null
                ? config.getMaximumRetryAttempts()
                : DEFAULT_MAXIMUM_RETRY_ATTEMPTS;
        int maxAge = config.getMaximumEventAgeInSeconds() != null
                ? config.getMaximumEventAgeInSeconds()
                : DEFAULT_MAXIMUM_EVENT_AGE_SECONDS;
        FunctionEventInvokeConfig.DestinationConfig destinations = config.getDestinationConfig();
        String onSuccess = destinations != null && destinations.getOnSuccess() != null
                ? destinations.getOnSuccess().getDestination()
                : null;
        String onFailure = destinations != null && destinations.getOnFailure() != null
                ? destinations.getOnFailure().getDestination()
                : null;
        return new AsyncInvokePolicy(invokedFunctionArn, retries, maxAge, onSuccess, onFailure);
    }
}
