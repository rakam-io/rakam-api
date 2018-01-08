package org.rakam.analysis.metadata;

import com.google.common.eventbus.EventBus;
import org.rakam.collection.SchemaField;
import org.rakam.plugin.SystemEvents;
import org.rakam.plugin.SystemEvents.ProjectCreatedEvent;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class AbstractMetastore
        implements Metastore {
    private final EventBus eventBus;

    public AbstractMetastore(EventBus eventBus) {
        this.eventBus = eventBus;
    }

    protected void onCreateProject(String project) {
        eventBus.post(new ProjectCreatedEvent(project));
    }

    protected void onDeleteProject(String project) {
        eventBus.post(new SystemEvents.ProjectDeletedEvent(project));
    }

    protected void onCreateCollection(String project, String collection, List<SchemaField> fields) {
        eventBus.post(new SystemEvents.CollectionCreatedEvent(project, collection, fields));
    }

    protected void onCreateCollectionField(String project, String collection, List<SchemaField> fields) {
        eventBus.post(new SystemEvents.CollectionFieldCreatedEvent(project, collection, fields));
    }

    public abstract List<SchemaField> getOrCreateCollectionFields(String project, String collection, Set<SchemaField> fields);

    @Override
    public Map<String, Stats> getStats(Collection<String> projects) {
        return projects.stream().collect(Collectors.toMap(e -> e, e -> new Stats()));
    }
}
