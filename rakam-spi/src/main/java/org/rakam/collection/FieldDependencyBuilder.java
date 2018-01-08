package org.rakam.collection;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.*;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Sets.newHashSet;


public class FieldDependencyBuilder {
    private final List<SchemaField> constantFields = Lists.newArrayList();
    private final Map<String, List<SchemaField>> dependentFields = Maps.newHashMap();

    public void addFields(List<SchemaField> fields) {
        checkFields(fields);
        constantFields.addAll(fields);
    }

    public void addFields(String dependentField, List<SchemaField> fields) {
        checkFields(fields);
        dependentFields.put(dependentField, fields);
    }

    private void checkFields(List<SchemaField> fields) {
        SchemaField[] collisions = fields.stream()
                .filter(newField -> constantFields.stream()
                        .anyMatch(f -> f.getName().equals(newField.getName()) && !f.getType().equals(newField.getType())))
                .toArray(SchemaField[]::new);
        checkState(collisions.length == 0, "Module field collides with existing field that has another type exists: ", Arrays.toString(collisions));

        collisions = dependentFields.values().stream()
                .flatMap(col -> col.stream())
                .filter(field -> fields.stream().anyMatch(f -> f.getName().equals(field.getName()) && !f.getType().equals(field.getType())))
                .toArray(SchemaField[]::new);

        checkState(collisions.length == 0, "Fields already exist in dependency table: ", Arrays.toString(collisions));
    }

    public FieldDependency build() {
        return new FieldDependency(newHashSet(constantFields), dependentFields);
    }

    public static class FieldDependency {
        public final Set<SchemaField> constantFields;
        public final Map<String, List<SchemaField>> dependentFields;

        public FieldDependency(Set<SchemaField> constantFields, Map<String, List<SchemaField>> dependentFields) {
            this.constantFields = Collections.unmodifiableSet(constantFields);
            this.dependentFields = Collections.unmodifiableMap(dependentFields);
        }
    }
}
