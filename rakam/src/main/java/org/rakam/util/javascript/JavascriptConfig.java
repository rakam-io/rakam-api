package org.rakam.util.javascript;

import io.airlift.configuration.Config;

public class JavascriptConfig {
    private boolean customEnabled = true;

    public boolean getCustomEnabled() {
        return customEnabled;
    }

    @Config("custom-javascript-enabled")
    public JavascriptConfig setCustomEnabled(boolean customEnabled) {
        this.customEnabled = customEnabled;
        return this;
    }
}
