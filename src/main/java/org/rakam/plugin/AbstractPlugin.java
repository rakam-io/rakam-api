package org.rakam.plugin;

/**
 * Created by buremba on 29/03/14.
 */
abstract class AbstractPlugin {
    public abstract String name();
    public abstract String description();
    public abstract void onModule();
    public abstract void onDestroy();
}
