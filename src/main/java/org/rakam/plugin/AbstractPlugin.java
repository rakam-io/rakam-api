package org.rakam.plugin;

/**
 * Created by buremba on 29/03/14.
 */
interface AbstractPlugin {
    public String name();

    public String description();

    public void onModule();

    public void onDestroy();
}