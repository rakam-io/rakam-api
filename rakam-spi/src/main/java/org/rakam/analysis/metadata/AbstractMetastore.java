package org.rakam.analysis.metadata;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.eventbus.EventBus;
import org.rakam.collection.FieldDependencyBuilder;
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
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static java.lang.String.format;

public abstract class AbstractMetastore implements Metastore {
    private final FieldDependency moduleFields;
    private final EventBus eventBus;
    private final Set<String> sourceFields;

    public AbstractMetastore(FieldDependency fieldDependency, EventBus eventBus) {
        this.moduleFields = fieldDependency;
        this.eventBus = eventBus;
        this.sourceFields = fieldDependency.dependentFields.keySet();
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

    protected void checkExistingSchema() {
        for (String project : getProjects()) {
            Map<String, List<SchemaField>> collections = getCollections(project);
            collections.forEach((collection, fields) -> {
                Set<SchemaField> collect = moduleFields.constantFields.stream()
                        .filter(constant ->
                                !fields.stream()
                                        .anyMatch(existing -> check(project, collection, constant, existing)))
                        .collect(Collectors.toSet());

                moduleFields.dependentFields.entrySet().stream().map(Map.Entry::getValue)
                        .forEach(values ->
                                values.stream().forEach(value ->
                                        fields.stream().forEach(field -> check(project, collection, field, value))));

                fields.forEach(field -> moduleFields.dependentFields.getOrDefault(field.getName(), ImmutableList.of()).stream()
                        .filter(dependentField -> !fields.stream()
                                .anyMatch(existing -> check(project, collection, existing, dependentField)))
                        .forEach(collect::add));

                if (!collect.isEmpty()) {
                    try {
                        getOrCreateCollectionFieldList(project, collection, collect);
                    }
                    catch (NotExistsException e) {
                        throw Throwables.propagate(e);
                    }
                }
            });
        }
    }

    private boolean check(String project, String collection, SchemaField existing, SchemaField moduleField) {
        if (existing.getName().equals(moduleField.getName())) {
            if (!existing.getType().equals(moduleField.getType())) {
                throw new IllegalStateException(format("Module field '%s' type does not match existing field in event of project %s.%s. Existing type: %s, Module field type: %s. \n" +
                                "Please change the schema manually of disable the module.",
                        existing.getName(), project, collection, existing.getType(), moduleField.getType()));
            }
            return true;
        }
        return false;
    }

    @Override
    public List<SchemaField> getOrCreateCollectionFieldList(String project, String collection, Set<SchemaField> fieldList) throws NotExistsException {
        HashSet<SchemaField> fields = new HashSet<>(fieldList);
        ValidationUtil.checkCollectionValid(collection);

        Iterator<SchemaField> it = fields.iterator();
        while (it.hasNext()) {
            SchemaField newField = it.next();
            if (sourceFields.contains(newField)) {
                it.remove();
            }
            if (newField.getName().startsWith("_")) {
                for (Map.Entry<String, List<SchemaField>> entry : moduleFields.dependentFields.entrySet()) {
                    Optional<SchemaField> collision = entry.getValue().stream()
                            .filter(e -> e.getName().equals(newField.getName()) && !e.getType().equals(e.getType()))
                            .findAny();
                    if(collision.isPresent()) {
                        throw new RakamException(format("Field %s.%s collides with one of the magic field with type %s", collection, newField.getName(), collision.get().getType()),
                                BAD_REQUEST);
                    }
                }
            }
        }
        moduleFields.constantFields.forEach(field -> addModuleField(fields, field));
        moduleFields.dependentFields.forEach((fieldName, field) -> addConditionalModuleField(fields, fieldName, field));
        return getOrCreateCollectionFields(project, collection, fields);
    }

    public abstract List<SchemaField> getOrCreateCollectionFields(String project, String collection, Set<SchemaField> fields);

    private void addConditionalModuleField(Set<SchemaField> fields, String field, List<SchemaField> newFields) {
        if (fields.stream().anyMatch(f -> f.getName().equals(field))) {
            newFields.forEach(newField -> addModuleField(fields, newField));
        }
    }

    private void addModuleField(Set<SchemaField> fields, SchemaField newField) {
        Iterator<SchemaField> iterator = fields.iterator();
        while (iterator.hasNext()) {
            SchemaField field = iterator.next();
            if (field.getName().equals(newField.getName())) {
                if (field.getType().equals(newField.getType())) {
                    return;
                } else {
                    iterator.remove();
                    break;
                }
            }
        }
        fields.add(newField);
    }

    @Override
    public Map<String, Stats> getStats(Collection<String> projects) {
        return projects.stream().collect(Collectors.toMap(e -> e, e -> new Stats()));
    }
}
