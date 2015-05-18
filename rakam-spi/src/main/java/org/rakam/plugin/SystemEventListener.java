package org.rakam.plugin;

/**
 * Created by buremba <Burak Emre Kabakcı> on 17/05/15 15:18.
 */
public interface SystemEventListener {
    default void onCreateProject(String project) {

    }

    default void onCreateCollection(String project, String collection) {

    }
}
