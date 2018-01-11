package org.rakam.collection;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.util.RakamException;
import org.rakam.util.ValidationUtil;

import static org.rakam.util.ValidationUtil.stripName;

public class SchemaField {
    private final String name;
    private final FieldType type;
    @JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
    private final String description;
    @JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
    private final String descriptiveName;
    @JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
    private final String category;

    @JsonCreator
    public SchemaField(@ApiParam("name") String name,
                       @ApiParam("type") FieldType type,
                       @ApiParam(value = "descriptiveName", required = false) String descriptiveName,
                       @ApiParam(value = "description", required = false) String description,
                       @ApiParam(value = "category", required = false) String category) {
        this.name = stripName(ValidationUtil.checkNotNull(name, "name"), "field name");
        this.type = ValidationUtil.checkNotNull(type, "type");
        this.descriptiveName = descriptiveName;
        this.description = description;
        this.category = category;
        if (this.name.isEmpty()) {
            throw new RakamException(String.format("Field name (%s) can't be empty string", this.name),
                    HttpResponseStatus.BAD_REQUEST);
        }
    }

    public SchemaField(String name, FieldType type) {
        this(name, type, null, null, null);
    }

    @JsonProperty
    public String getName() {
        return name;
    }

    @JsonProperty
    public FieldType getType() {
        return type;
    }

    public String getCategory() {
        return category;
    }

    @JsonProperty
    public String getDescriptiveName() {
        if (descriptiveName == null) {
            String replace = name.replace("_", " ").trim();
            return Character.toUpperCase(replace.charAt(0)) + replace.substring(1);
        }
        return descriptiveName;
    }

    @JsonProperty
    @JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
    public String getDescription() {
        return description;
    }

    @Override
    public String toString() {
        return "SchemaField{" +
                "name='" + name + '\'' +
                ", type=" + type +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SchemaField)) {
            return false;
        }

        SchemaField that = (SchemaField) o;

        if (!name.equals(that.name)) {
            return false;
        }
        if (type != that.type) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + type.hashCode();
        return result;
    }
}
