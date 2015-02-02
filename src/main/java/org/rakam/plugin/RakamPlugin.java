package org.rakam.plugin;

/**
 * Created by buremba on 29/03/14.
 */
public interface RakamPlugin {
    public String name();

    public String description();

    public void register();

    public void onDestroy();
}