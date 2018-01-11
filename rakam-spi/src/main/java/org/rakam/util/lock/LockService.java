package org.rakam.util.lock;

public interface LockService {
    Lock tryLock(String name);

    @FunctionalInterface
    interface Lock {
        void release();
    }
}
