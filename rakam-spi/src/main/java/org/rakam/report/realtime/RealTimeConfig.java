package org.rakam.report.realtime;

import io.airlift.configuration.Config;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;


public class RealTimeConfig {
    private boolean enabled;
    private Duration windowInterval = Duration.valueOf("120s");
    private Duration slideInterval = Duration.valueOf("10s");

    public boolean isRealtimeModuleEnabled() {
        return enabled;
    }

    @Config("real-time.enabled")
    public RealTimeConfig setRealtimeModuleEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    public Duration getWindowInterval() {
        return windowInterval;
    }

    @Config("realtime.window.interval")
    public RealTimeConfig setWindowInterval(String windowInterval) {
        this.windowInterval = Duration.valueOf(windowInterval);
        return this;
    }

    @MinDuration("1s")
    public Duration getSlideInterval() {
        return slideInterval;
    }

    @Config("realtime.window.interval")
    public RealTimeConfig setSlideInterval(String slideInterval) {
        this.slideInterval = Duration.valueOf(slideInterval);
        return this;
    }
}
