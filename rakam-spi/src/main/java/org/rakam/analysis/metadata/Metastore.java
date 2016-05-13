package org.rakam.analysis.metadata;

import org.rakam.util.NotExistsException;
import org.rakam.collection.SchemaField;

import java.util.List;
import java.util.Map;
import java.util.Set;


public interface Metastore {
    Map<String, Set<String>> getAllCollections();

    Map<String, List<SchemaField>> getCollections(String project);

    Set<String> getCollectionNames(String project);


    void createProject(String project);

    Set<String> getProjects();

    List<SchemaField> getCollection(String project, String collection);

    List<SchemaField> getOrCreateCollectionFieldList(String project, String collection, Set<SchemaField> fields) throws NotExistsException;

    void deleteProject(String project);

    Map<String, Stats> getStats(List<String> projects);

    default void setup() {}

    class Stats {
        public long allEvents;
        public long monthlyEvents;
        public long dailyEvents;

        public Stats() {
        }

        public Stats(long allEvents, long monthlyEvents, long dailyEvents) {
            this.allEvents = allEvents;
            this.monthlyEvents = monthlyEvents;
            this.dailyEvents = dailyEvents;
        }
    }

    final class Project {
        public final String name;
        public final String apiUrl;

        public Project(String name, String apiUrl) {
            this.name = name;
            this.apiUrl = apiUrl;
        }
    }
}