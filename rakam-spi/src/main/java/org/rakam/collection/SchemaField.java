package org.rakam.collection;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.Locale;


public class SchemaField {
    private final String name;
    private final FieldType type;
    @JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
    private final Boolean nullable;
    @JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
    private final Boolean unique;
    @JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
    private final String description;
    @JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
    private final String descriptiveName;
    @JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
    private final String category;

    @JsonCreator
    public SchemaField(@JsonProperty("name") String name,
                       @JsonProperty("type") FieldType type,
                       @JsonProperty("nullable") Boolean nullable,
                       @JsonProperty("unique") Boolean unique,
                       @JsonProperty("descriptiveName") String descriptiveName,
                       @JsonProperty("description") String description,
                       @JsonProperty("category") String category) {
        this.name = name.toLowerCase(Locale.ENGLISH);
        this.type = type;
        this.nullable = nullable;
        this.unique = unique;
        this.descriptiveName = descriptiveName;
        this.description = description;
        this.category = category;
    }

    public SchemaField(String name, FieldType type, Boolean nullable) {
        this(name, type, nullable, null, null, null, null);
    }

    @JsonProperty
    public String getName() {
        return name;
    }

    @JsonProperty
    public FieldType getType() {
        return type;
    }

    @JsonProperty
    public Boolean isUnique() {
        return unique;
    }

    @JsonProperty
    public Boolean isNullable() {
        return nullable;
    }

    public String getCategory() {
        return category;
    }

    @JsonProperty
    public String getDescriptiveName() {
        if(descriptiveName == null) {
            String replace = name.replace("_", " ").trim();
            return Character.toUpperCase(replace.charAt(0)) + replace.substring(1);
        }
        return descriptiveName;
    }

    @JsonProperty
    @JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
    public String getDescription() {
        return description;
    }

    @Override
    public String toString() {
        return "SchemaField{" +
                "name='" + name + '\'' +
                ", type=" + type +
                ", nullable=" + nullable +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SchemaField)) return false;

        SchemaField that = (SchemaField) o;

        if (nullable != that.nullable) return false;
        if (!name.equals(that.name)) return false;
        if (type != that.type) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + type.hashCode();
        result = 31 * result + (nullable ? 1 : 0);
        return result;
    }
}
