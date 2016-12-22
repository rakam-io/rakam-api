package org.rakam.plugin.tasks;

public interface LockService
{
    Lock tryLock(String name);

    @FunctionalInterface
    interface Lock {
        void release();
    }
}
