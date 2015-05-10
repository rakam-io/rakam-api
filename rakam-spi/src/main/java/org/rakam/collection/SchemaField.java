package org.rakam.collection;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Created by buremba <Burak Emre Kabakcı> on 10/03/15 03:40.
 */
public class SchemaField {
    private final String name;
    private final FieldType type;
    private final boolean nullable;

    @JsonCreator
    public SchemaField(@JsonProperty("name") String name,
                       @JsonProperty("type") FieldType type,
                       @JsonProperty("nullable") boolean nullable) {
        this.name = name;
        this.type = type;
        this.nullable = nullable;
    }

    public String getName() {
        return name;
    }

    public FieldType getType() {
        return type;
    }

    public boolean isNullable() {
        return nullable;
    }

    @Override
    public String toString() {
        return "SchemaField{" +
                "name='" + name + '\'' +
                ", type=" + type +
                ", nullable=" + nullable +
                '}';
    }
}
