package org.rakam.sql;

import org.rakam.collection.FieldType;

/**
 * Created by buremba <Burak Emre Kabakcı> on 11/04/15 08:31.
 */
public class NamedParameterValue {
    public final FieldType type;
    public final Object value;

    public NamedParameterValue(FieldType type, Object value) {
        this.type = type;
        this.value = value;
    }
}
