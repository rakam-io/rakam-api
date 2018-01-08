package org.rakam.analysis.metadata;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import org.rakam.collection.FieldDependencyBuilder;
import org.rakam.collection.SchemaField;
import org.rakam.util.NotExistsException;
import org.rakam.util.RakamException;

import javax.inject.Inject;
import java.util.*;
import java.util.stream.Collectors;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static java.lang.String.format;

public class SchemaChecker {
    private final Metastore metastore;
    private final FieldDependencyBuilder.FieldDependency fieldDependency;

    @Inject
    public SchemaChecker(Metastore metastore, FieldDependencyBuilder.FieldDependency fieldDependency) {
        this.metastore = metastore;
        this.fieldDependency = fieldDependency;
    }

    public HashSet<SchemaField> checkNewFields(String collection, Set<SchemaField> newFields) {
        HashSet<SchemaField> fields = new HashSet<>(newFields);

        Iterator<SchemaField> it = fields.iterator();
        while (it.hasNext()) {
            SchemaField newField = it.next();
            if (fieldDependency.dependentFields.containsKey(newField)) {
                it.remove();
            }
            if (newField.getName().startsWith("_")) {
                for (Map.Entry<String, List<SchemaField>> entry : fieldDependency.dependentFields.entrySet()) {
                    Optional<SchemaField> collision = entry.getValue().stream()
                            .filter(e -> e.getName().equals(newField.getName()) && !e.getType().equals(e.getType()))
                            .findAny();
                    if (collision.isPresent()) {
                        throw new RakamException(format("Field %s.%s collides with one of the magic field with type %s", collection, newField.getName(), collision.get().getType()),
                                BAD_REQUEST);
                    }
                }
            }
            if (newField.getName().equals("$server_time")) {
                throw new RakamException("$server_time is reserved as a system attribute", BAD_REQUEST);
            }
        }
        fieldDependency.constantFields.forEach(field -> addModuleField(fields, field));
        fieldDependency.dependentFields.forEach((fieldName, field) -> addConditionalModuleField(fields, fieldName, field));

        return fields;
    }

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

    //    @PostConstruct
    protected void checkExistingSchema() {
        for (String project : metastore.getProjects()) {
            Map<String, List<SchemaField>> collections = metastore.getCollections(project);
            collections.forEach((collection, fields) -> {
                Set<SchemaField> collect = fieldDependency.constantFields.stream()
                        .filter(constant ->
                                !fields.stream()
                                        .anyMatch(existing -> check(project, collection, constant, existing)))
                        .collect(Collectors.toSet());

                fieldDependency.dependentFields.entrySet().stream().map(Map.Entry::getValue)
                        .forEach(values ->
                                values.stream().forEach(value ->
                                        fields.stream().forEach(field -> check(project, collection, field, value))));

                fields.forEach(field -> fieldDependency.dependentFields.getOrDefault(field.getName(), ImmutableList.of()).stream()
                        .filter(dependentField -> !fields.stream()
                                .anyMatch(existing -> check(project, collection, existing, dependentField)))
                        .forEach(collect::add));

                if (!collect.isEmpty()) {
                    try {
                        metastore.getOrCreateCollectionFields(project, collection, collect);
                    } catch (NotExistsException e) {
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
}
