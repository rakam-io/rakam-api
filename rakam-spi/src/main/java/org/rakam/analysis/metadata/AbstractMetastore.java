package org.rakam.analysis.metadata;

import com.google.common.eventbus.EventBus;
import org.rakam.collection.FieldDependencyBuilder.FieldDependency;
import org.rakam.collection.SchemaField;
import org.rakam.plugin.SystemEvents;
import org.rakam.plugin.SystemEvents.ProjectCreatedEvent;
import org.rakam.util.NotExistsException;
import org.rakam.util.RakamException;
import org.rakam.util.ValidationUtil;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static java.lang.String.format;

public abstract class AbstractMetastore
        implements Metastore
{
    private final EventBus eventBus;

    public AbstractMetastore(EventBus eventBus)
    {
        this.eventBus = eventBus;
    }

    protected void onCreateProject(String project)
    {
        eventBus.post(new ProjectCreatedEvent(project));
    }

    protected void onDeleteProject(String project)
    {
        eventBus.post(new SystemEvents.ProjectDeletedEvent(project));
    }

    protected void onCreateCollection(String project, String collection, List<SchemaField> fields)
    {
        eventBus.post(new SystemEvents.CollectionCreatedEvent(project, collection, fields));
    }

    protected void onCreateCollectionField(String project, String collection, List<SchemaField> fields)
    {
        eventBus.post(new SystemEvents.CollectionFieldCreatedEvent(project, collection, fields));
    }

    @Override
    public List<SchemaField> getOrCreateCollectionFieldList(String project, String collection, Set<SchemaField> fieldList)
            throws NotExistsException
    {
        ValidationUtil.checkCollectionValid(collection);
        return getOrCreateCollectionFields(project, collection, fieldList);
    }

    public abstract List<SchemaField> getOrCreateCollectionFields(String project, String collection, Set<SchemaField> fields);


    @Override
    public Map<String, Stats> getStats(Collection<String> projects)
    {
        return projects.stream().collect(Collectors.toMap(e -> e, e -> new Stats()));
    }
}
