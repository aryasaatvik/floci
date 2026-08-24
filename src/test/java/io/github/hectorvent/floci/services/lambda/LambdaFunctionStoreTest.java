package io.github.hectorvent.floci.services.lambda;

import io.github.hectorvent.floci.core.storage.AccountAwareStorageBackend;
import io.github.hectorvent.floci.services.lambda.model.LambdaFunction;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

class LambdaFunctionStoreTest {

    @Test
    void listAllAccountsIncludesFunctionsOutsideTheDefaultAccount() {
        AccountAwareStorageBackend<LambdaFunction> backend =
                AccountAwareStorageBackend.inMemory("000000000000");
        LambdaFunction first = function("first");
        LambdaFunction second = function("second");
        backend.putForAccount("111111111111", "lambda::us-east-1::first::$LATEST", first);
        backend.putForAccount("222222222222", "lambda::us-east-1::second::$LATEST", second);
        LambdaFunctionStore store = new LambdaFunctionStore(backend);

        assertEquals(Set.of(first, second), Set.copyOf(store.listAllAccounts()));
    }

    private static LambdaFunction function(String name) {
        LambdaFunction function = new LambdaFunction();
        function.setFunctionName(name);
        return function;
    }
}
