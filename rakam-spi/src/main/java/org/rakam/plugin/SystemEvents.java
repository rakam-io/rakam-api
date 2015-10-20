package org.rakam.plugin;

public class SystemEvents {
    public static class ProjectCreatedEvent {
        public final String project;

        public ProjectCreatedEvent(String project) {
            this.project = project;
        }
    }

    public static class CollectionCreatedEvent {
        public final String project;
        public final String collection;

        public CollectionCreatedEvent(String project, String collection) {
            this.project = project;
            this.collection = collection;
        }
    }
}
