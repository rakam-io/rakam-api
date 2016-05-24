package org.rakam.ui;

public class UIEvents {
    public static class ProjectCreatedEvent {
        public final int project;

        public ProjectCreatedEvent(int project) {
            this.project = project;
        }
    }

    public static class ProjectDeletedEvent {
        public final int project;

        public ProjectDeletedEvent(int project) {
            this.project = project;
        }
    }
}
