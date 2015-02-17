package org.rakam.realtime;

import io.airlift.configuration.Config;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

/**
 * Created by buremba <Burak Emre Kabakcı> on 15/02/15 13:42.
 */
public class RealTimeConfig {
    private Duration timeout = Duration.valueOf("45s");
    private Duration updateInterval = Duration.valueOf("5s");

    @MinDuration("1s")
    public Duration getTimeout()
    {
        return timeout;
    }

    @Config("realtime.timeout")
    public RealTimeConfig setTimeout(String timeout)
    {
        if(timeout != null)
            this.timeout = Duration.valueOf(timeout);
        return this;
    }

    @MinDuration("1s")
    public Duration getUpdateInterval()
    {
        return updateInterval;
    }

    @Config("realtime.update-interval")
    public RealTimeConfig setUpdateInterval(String interval)
    {
        if(interval != null)
            this.updateInterval = Duration.valueOf(interval);
        return this;
    }
}
