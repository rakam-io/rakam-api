package org.rakam.analysis.metadata;

import org.rakam.collection.SchemaField;
import org.rakam.util.NotExistsException;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;


public interface Metastore {
    Map<String, List<SchemaField>> getCollections(String project);

    Set<String> getCollectionNames(String project);

    void createProject(String project);

    Set<String> getProjects();

    List<SchemaField> getCollection(String project, String collection);

    List<SchemaField> getOrCreateCollectionFieldList(String project, String collection, Set<SchemaField> fields) throws NotExistsException;

    void deleteProject(String project);

    Map<String, Stats> getStats(Collection<String> projects);

    default void setup() {}

    class Stats {
        public Long allEvents;
        public Long monthlyEvents;
        public Long dailyEvents;

        public Stats() {
        }

        public Stats(Long allEvents, Long monthlyEvents, Long dailyEvents) {
            this.allEvents = allEvents;
            this.monthlyEvents = monthlyEvents;
            this.dailyEvents = dailyEvents;
        }
    }
}