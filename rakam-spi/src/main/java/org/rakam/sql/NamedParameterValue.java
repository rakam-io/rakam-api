package org.rakam.sql;

import org.rakam.collection.FieldType;


public class NamedParameterValue {
    public final FieldType type;
    public final Object value;

    public NamedParameterValue(FieldType type, Object value) {
        this.type = type;
        this.value = value;
    }
}
