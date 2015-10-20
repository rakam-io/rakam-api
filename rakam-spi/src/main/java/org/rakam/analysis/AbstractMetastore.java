package org.rakam.analysis;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.eventbus.EventBus;
import org.rakam.collection.SchemaField;
import org.rakam.collection.event.FieldDependencyBuilder;
import org.rakam.collection.event.metastore.Metastore;
import org.rakam.plugin.EventMapper;
import org.rakam.plugin.SystemEvents;

import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class AbstractMetastore implements Metastore {
    private final FieldDependencyBuilder.FieldDependency moduleFields;
    private final EventBus eventBus;

    public AbstractMetastore(Set<EventMapper> eventMappers, EventBus eventBus) {
        // TODO: get all collections of the projects and check there is a collision between existing fields in collections
        // TODO: and fields that are added by installed modules
        FieldDependencyBuilder builder = new FieldDependencyBuilder();
        eventMappers.stream().forEach(mapper -> mapper.addFieldDependency(builder));

        this.moduleFields = builder.build();
        this.eventBus = eventBus;
    }

    protected void onCreateProject(String project) {
        eventBus.post(new SystemEvents.ProjectCreatedEvent(project));
    }

    protected void onCreateCollection(String project, String collection) {
        eventBus.post(new SystemEvents.CollectionCreatedEvent(project, collection));
    }

    protected void checkExistingSchema() {
        getProjects().forEach(project ->
                getCollections(project).forEach((collection, fields) -> {
                    List<SchemaField> collect = moduleFields.constantFields.stream()
                            .filter(constant ->
                                    !fields.stream()
                                            .anyMatch(existing -> check(constant, existing))).collect(Collectors.toList());

                    fields.forEach(field -> moduleFields.dependentFields.getOrDefault(field.getName(), ImmutableList.of()).stream()
                            .filter(dependentField -> !fields.stream()
                                    .anyMatch(existing -> check(existing, dependentField)))
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

    private boolean check(SchemaField existing, SchemaField moduleField) {
        if(existing.getName().equals(moduleField.getName())) {
            if (!existing.getType().equals(moduleField.getType())) {
                throw new IllegalStateException(String.format("Module field '%s' type does not match existing field in event. Existing type: %s, Module field type: %s",
                        existing.getName(), existing.getType(), moduleField.getType()));
            }
            return true;
        }
        return false;
    }

    @Override
    public List<SchemaField> getOrCreateCollectionFieldList(String project, String collection, List<SchemaField> fields) throws ProjectNotExistsException {
        moduleFields.constantFields.forEach(field -> addModuleField(fields, field));
        moduleFields.dependentFields.forEach((fieldName, field) -> addConditionalModuleField(fields, fieldName, field));
        return getOrCreateCollectionFields(project, collection.toLowerCase(Locale.ENGLISH), fields);
    }

    public abstract List<SchemaField> getOrCreateCollectionFields(String project, String collection, List<SchemaField> fields);

    private void addConditionalModuleField(List<SchemaField> fields, String fieldName, List<SchemaField> newFields) {
        if (fields.stream().anyMatch(f -> f.getName().equals(fieldName))) {
            newFields.forEach(newField -> addModuleField(fields, newField));
        }
    }

    private void addModuleField(List<SchemaField> fields, SchemaField newField) {
        for (int i = 0; i < fields.size(); i++) {
            SchemaField field = fields.get(i);
            if(field.getName().equals(newField.getName())) {
                if(field.getType().equals(newField.getType())) {
                    return;
                }else {
                    fields.remove(i);
                    break;
                }
            }
        }
        fields.add(newField);
    }
}
