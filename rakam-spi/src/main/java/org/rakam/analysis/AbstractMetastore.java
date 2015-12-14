package org.rakam.analysis;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.eventbus.EventBus;
import org.rakam.collection.SchemaField;
import org.rakam.collection.event.FieldDependencyBuilder;
import org.rakam.collection.event.metastore.Metastore;
import org.rakam.plugin.SystemEvents;

import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class AbstractMetastore implements Metastore {
    private final FieldDependencyBuilder.FieldDependency moduleFields;
    private final EventBus eventBus;
    private final Set<String> sourceFields;

    public AbstractMetastore(FieldDependencyBuilder.FieldDependency fieldDependency, EventBus eventBus) {
        this.moduleFields = fieldDependency;
        this.eventBus = eventBus;
        this.sourceFields = fieldDependency.dependentFields.keySet();
    }

    protected void onCreateProject(String project) {
        eventBus.post(new SystemEvents.ProjectCreatedEvent(project));
    }

    protected void onCreateCollection(String project, String collection, List<SchemaField> fields) {
        eventBus.post(new SystemEvents.CollectionCreatedEvent(project, collection, fields));
    }

    protected void onCreateCollectionField(String project, String collection, List<SchemaField> fields) {
        eventBus.post(new SystemEvents.CollectionFieldCreatedEvent(project, collection, fields));
    }

    protected void checkExistingSchema() {
        getProjects().forEach(project ->
                getCollections(project).forEach((collection, fields) -> {
                    Set<SchemaField> collect = moduleFields.constantFields.stream()
                            .filter(constant ->
                                    !fields.stream()
                                            .anyMatch(existing -> check(project, constant, existing)))
                            .collect(Collectors.toSet());

                    moduleFields.dependentFields.entrySet().stream().map(Map.Entry::getValue)
                            .forEach(values ->
                                    values.stream().forEach(value ->
                                            fields.stream().forEach(field -> check(project, field, value))));

                    fields.forEach(field -> moduleFields.dependentFields.getOrDefault(field.getName(), ImmutableList.of()).stream()
                            .filter(dependentField -> !fields.stream()
                                    .anyMatch(existing -> check(project, existing, dependentField)))
                            .forEach(collect::add));

                    if(!collect.isEmpty()) {
                        try {
                            getOrCreateCollectionFieldList(project, collection, collect);
                        } catch (ProjectNotExistsException e) {
                            throw Throwables.propagate(e);
                        }
                    }
                }));
    }

    private boolean check(String project, SchemaField existing, SchemaField moduleField) {
        if(existing.getName().equals(moduleField.getName())) {
            if (!existing.getType().equals(moduleField.getType())) {
                throw new IllegalStateException(String.format("Module field '%s' type does not match existing field in event of project %s. Existing type: %s, Module field type: %s. \n" +
                                "Please change the schema manually of disable the module.",
                        existing.getName(), project, existing.getType(), moduleField.getType()));
            }
            return true;
        }
        return false;
    }

    @Override
    public List<SchemaField> getOrCreateCollectionFieldList(String project, String collection, Set<SchemaField> fields) throws ProjectNotExistsException {
        Iterator<SchemaField> it = fields.iterator();
        while(it.hasNext()) {
            if(sourceFields.contains(it.next())){
                it.remove();
            }
        }
        moduleFields.constantFields.forEach(field -> addModuleField(fields, field));
        moduleFields.dependentFields.forEach((fieldName, field) -> addConditionalModuleField(fields, fieldName, field));
        return getOrCreateCollectionFields(project, collection.toLowerCase(Locale.ENGLISH), fields);
    }

    public abstract List<SchemaField> getOrCreateCollectionFields(String project, String collection, Set<SchemaField> fields);

    private void addConditionalModuleField(Set<SchemaField> fields, String field, List<SchemaField> newFields) {
        if (fields.stream().anyMatch(f -> f.getName().equals(field))) {
            newFields.forEach(newField -> addModuleField(fields, newField));
        }
    }

    private void addModuleField(Set<SchemaField> fields, SchemaField newField) {
        Iterator<SchemaField> iterator = fields.iterator();
        while(iterator.hasNext()) {
            SchemaField field = iterator.next();
            if(field.getName().equals(newField.getName())) {
                if(field.getType().equals(newField.getType())) {
                    return;
                }else {
                    iterator.remove();
                    break;
                }
            }
        }
        fields.add(newField);
    }
}
