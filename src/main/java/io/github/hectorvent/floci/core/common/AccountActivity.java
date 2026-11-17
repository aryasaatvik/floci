package io.github.hectorvent.floci.core.common;

import jakarta.enterprise.context.ApplicationScoped;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/** Coordinates an exact-account reclaim with account-scoped work admitted by this process. */
@ApplicationScoped
public class AccountActivity {

    private static final int RECLAIMING = -1;

    private final ConcurrentHashMap<String, Integer> activity = new ConcurrentHashMap<>();

    public boolean isBusy(String accountId) {
        return activity.containsKey(accountId);
    }

    public boolean isReclaiming(String accountId) {
        return activity.getOrDefault(accountId, 0) == RECLAIMING;
    }

    public Permit tryWork(String accountId) {
        int current = activity.compute(accountId, (account, count) ->
                count == null ? 1 : count == RECLAIMING ? count : count + 1);
        return current == RECLAIMING ? null : permit(() ->
                activity.compute(accountId, (account, count) -> count == 1 ? null : count - 1));
    }

    public Permit beginWork(String accountId) {
        Permit permit = tryWork(accountId);
        if (permit == null) {
            throw new AwsException("ConflictException", "Account " + accountId + " is being reclaimed", 409);
        }
        return permit;
    }

    public Permit tryReclaim(String accountId) {
        if (activity.putIfAbsent(accountId, RECLAIMING) != null) {
            return null;
        }
        return permit(() -> activity.remove(accountId, RECLAIMING));
    }

    private static Permit permit(Runnable release) {
        AtomicBoolean open = new AtomicBoolean(true);
        return () -> {
            if (open.compareAndSet(true, false)) {
                release.run();
            }
        };
    }

    public interface Permit extends AutoCloseable {
        @Override
        void close();
    }
}
