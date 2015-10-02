package org.rakam.plugin;


public interface SystemEventListener {
    default void onCreateProject(String project) {

    }

    default void onCreateCollection(String project, String collection) {

    }
}
