package org.rakam.collection;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.util.RakamException;
import org.rakam.util.ValidationUtil;

import java.util.Locale;

public class SchemaField
{
    private final String name;
    private final FieldType type;
    @JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
    private final Boolean unique;
    @JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
    private final String description;
    @JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
    private final String descriptiveName;
    @JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
    private final String category;

    @JsonCreator
    public SchemaField(@JsonProperty("name") String name,
            @JsonProperty("type") FieldType type,
            @JsonProperty("unique") Boolean unique,
            @JsonProperty("descriptiveName") String descriptiveName,
            @JsonProperty("description") String description,
            @JsonProperty("category") String category)
    {
        this.name = stripName(ValidationUtil.checkNotNull(name, "name"));
        this.type = ValidationUtil.checkNotNull(type, "type");
        this.unique = unique;
        this.descriptiveName = descriptiveName;
        this.description = description;
        this.category = category;
        if (this.name.isEmpty()) {
            throw new RakamException(String.format("Field name (%s) can't be empty string", this.name),
                    HttpResponseStatus.BAD_REQUEST);
        }
    }

    public static String stripName(String name)
    {
        StringBuilder builder = new StringBuilder(name.length());
        for (int i = 0; i < name.length(); i++) {
            char charAt = name.charAt(i);
            if (charAt == '"' || (i == 0 && charAt == ' ')) {
                continue;
            }

            if (Character.isUpperCase(charAt)) {
                if (i > 0) {
                    if (Character.isLowerCase(name.charAt(i - 1))) {
                        builder.append("_");
                    }
                }

                builder.append(Character.toLowerCase(charAt));
            }
            else {
                builder.append(charAt);
            }
        }

        if(builder.length() == 0) {
            throw new RakamException("Invalid collection: "+name, HttpResponseStatus.BAD_REQUEST);
        }

        int lastIdx = builder.length() - 1;
        if(builder.charAt(lastIdx) == ' ') {
            builder.deleteCharAt(lastIdx);
        }

        return builder.toString();
    }

    public SchemaField(String name, FieldType type)
    {
        this(name, type, null, null, null, null);
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public FieldType getType()
    {
        return type;
    }

    @JsonProperty
    public Boolean isUnique()
    {
        return unique;
    }

    public String getCategory()
    {
        return category;
    }

    @JsonProperty
    public String getDescriptiveName()
    {
        if (descriptiveName == null) {
            String replace = name.replace("_", " ").trim();
            return Character.toUpperCase(replace.charAt(0)) + replace.substring(1);
        }
        return descriptiveName;
    }

    @JsonProperty
    @JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
    public String getDescription()
    {
        return description;
    }

    @Override
    public String toString()
    {
        return "SchemaField{" +
                "name='" + name + '\'' +
                ", type=" + type +
                '}';
    }

    @Override
    public boolean equals(Object o)
    {
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
    public int hashCode()
    {
        int result = name.hashCode();
        result = 31 * result + type.hashCode();
        return result;
    }
}
