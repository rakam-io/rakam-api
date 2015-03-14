package org.rakam.scheduler;

import java.util.concurrent.TimeUnit;

/**
 * Created by buremba <Burak Emre Kabakcı> on 11/03/15 02:20.
 */
public interface Scheduler {
    void scheduleAtFixedRate(String schedulerName, Runnable runnable, long period, TimeUnit unit);
}
