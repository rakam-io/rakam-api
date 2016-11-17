package org.rakam.analysis;

import org.rakam.collection.FieldType;

public enum InternalConfig
{
    USER_TYPE(FieldType.STRING), FIXED_SCHEMA(FieldType.BOOLEAN);

    private final FieldType type;

    InternalConfig(FieldType type)
    {
        this.type = type;
    }

    public FieldType getType()
    {
        return type;
    }
}
