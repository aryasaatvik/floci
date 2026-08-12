package io.github.hectorvent.floci.services.lambda;

import io.github.hectorvent.floci.core.storage.AccountAwareStorageBackend;
import io.github.hectorvent.floci.services.lambda.model.LambdaFunction;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

class LambdaFunctionStoreTest {

    @Test
    void listAllAccountsSupportsProcessWideLifecycleReconciliation() {
        AccountAwareStorageBackend<LambdaFunction> backend =
                AccountAwareStorageBackend.inMemory("000000000000");
        backend.putForAccount("111111111111", "lambda::us-east-1::first::$LATEST", function("first"));
        backend.putForAccount("222222222222", "lambda::eu-west-1::second::$LATEST", function("second"));

        Set<String> names = new LambdaFunctionStore(backend).listAllAccounts().stream()
                .map(LambdaFunction::getFunctionName)
                .collect(Collectors.toSet());

        assertEquals(Set.of("first", "second"), names);
    }

    private static LambdaFunction function(String name) {
        LambdaFunction function = new LambdaFunction();
        function.setFunctionName(name);
        return function;
    }
}
