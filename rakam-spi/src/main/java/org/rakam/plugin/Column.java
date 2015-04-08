package org.rakam.plugin;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;

/**
* Created by buremba <Burak Emre Kabakcı> on 18/03/15 14:32.
*/
public class Column extends SchemaField {
    private final String name;
    private final FieldType type;
    private final boolean unique;

    @JsonCreator
    public Column(@JsonProperty("name") String name,
                  @JsonProperty("type") FieldType type,
                  @JsonProperty("unique") boolean unique) {
        super(name, type, true);
        this.name = name;
        this.type = type;
        this.unique = unique;
    }

    public String getName() {
        return name;
    }

    public FieldType getType() {
        return type;
    }

    public boolean isUnique() {
        return unique;
    }
}
