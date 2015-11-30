package org.rakam.report.abtesting;

import org.rakam.plugin.ProjectItem;

import java.util.List;

public class ABTestingReport implements ProjectItem {
    public final String project;
    public final String name;
    public final List<Variant> query;
    public final Goal goal;

    public ABTestingReport(String project, String name, List<Variant> query, Goal goal) {
        this.project = project;
        this.name = name;
        this.query = query;
        this.goal = goal;
    }

    public static class Goal {
        public final String collection;
        public final String filter;

        public Goal(String collection, String filter) {
            this.collection = collection;
            this.filter = filter;
        }
    }
    public static class Variant {
        public final String name;
        public final Object data;

        public Variant(String name, Object data) {
            this.name = name;
            this.data = data;
        }
    }

    @Override
    public String project() {
        return project;
    }
}
