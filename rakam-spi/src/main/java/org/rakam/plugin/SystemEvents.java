package org.rakam.plugin;

import org.rakam.collection.SchemaField;

import java.util.List;

public class SystemEvents {
    public static class ProjectCreatedEvent {
        public final String project;

        public ProjectCreatedEvent(String project) {
            this.project = project;
        }
    }

    public static class ProjectDeletedEvent {
        public final String project;

        public ProjectDeletedEvent(String project) {
            this.project = project;
        }
    }

    public static class CollectionCreatedEvent {
        public final String project;
        public final String collection;
        public final List<SchemaField> fields;

        public CollectionCreatedEvent(String project, String collection, List<SchemaField> fields) {
            this.project = project;
            this.collection = collection;
            this.fields = fields;
        }
    }

    public static class CollectionFieldCreatedEvent {
        public final String project;
        public final String collection;
        public final List<SchemaField> fields;

        public CollectionFieldCreatedEvent(String project, String collection, List<SchemaField> fields) {
            this.project = project;
            this.collection = collection;
            this.fields = fields;
        }
    }
}
