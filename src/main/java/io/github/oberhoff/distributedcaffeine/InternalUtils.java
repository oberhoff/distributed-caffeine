package io.github.oberhoff.distributedcaffeine;

import static java.util.Objects.requireNonNull;

class InternalUtils {

    static <T> void requireNonNullOnCondition(boolean condition, T object, String message) {
        if (condition) {
            requireNonNull(object, message);
        }
    }

    static void runFailable(FailableRunnable failableRunnable) {
        try {
            failableRunnable.run();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static <T> T getFailable(FailableSupplier<T> failableSupplier) {
        try {
            return failableSupplier.get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @FunctionalInterface
    interface FailableRunnable {

        void run() throws Exception;
    }

    @FunctionalInterface
    interface FailableSupplier<T> {

        T get() throws Exception;
    }
}
