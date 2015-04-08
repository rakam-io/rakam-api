package org.rakam.collection.event;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.rakam.collection.SchemaField;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;

/**
 * Created by buremba <Burak Emre Kabakcı> on 14/03/15 22:24.
 */
public class FieldDependencyBuilder {
    private final List<SchemaField> constantFields = Lists.newArrayList();
    private final Map<String, List<SchemaField>> dependentFields = Maps.newHashMap();

    public void addFields(List<SchemaField> fields) {
        checkFields(fields);
        constantFields.addAll(fields);
    }

    public void addFields(String dependentFieldName, List<SchemaField> fields) {
        checkFields(fields);
        dependentFields.put(dependentFieldName, fields);
    }

    private void checkFields(List<SchemaField> fields) {
        SchemaField[] collisions = fields.stream()
                .filter(newField -> constantFields.stream().anyMatch(f -> f.getName().equals(newField.getName())))
                .toArray(SchemaField[]::new);
        checkState(collisions.length == 0, "Fields already exists: ", Arrays.toString(collisions));

        collisions = dependentFields.values().stream()
                .flatMap(col -> col.stream())
                .filter(field -> fields.stream().anyMatch(f -> f.getName().equals(field.getName())))
                .toArray(SchemaField[]::new);

        checkState(collisions.length == 0, "Fields already exists in dependency table: ", Arrays.toString(collisions));
    }

    FieldDependency build() {
        return new FieldDependency(constantFields, dependentFields);
    }

    public static class FieldDependency {
        public final List<SchemaField> constantFields;
        public final Map<String, List<SchemaField>> dependentFields;

        public FieldDependency(List<SchemaField> constantFields, Map<String, List<SchemaField>> dependentFields) {
            this.constantFields = Collections.unmodifiableList(constantFields);
            this.dependentFields = Collections.unmodifiableMap(dependentFields);
        }
    }
}
