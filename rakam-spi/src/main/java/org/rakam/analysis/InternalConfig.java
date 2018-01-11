package org.rakam.analysis;

import org.rakam.collection.FieldType;

public enum InternalConfig {
    USER_TYPE(FieldType.STRING, false), FIXED_SCHEMA(FieldType.BOOLEAN, true);

    private final FieldType type;
    private final boolean dynamic;

    InternalConfig(FieldType type, boolean dynamic) {
        this.type = type;
        this.dynamic = dynamic;
    }

    public FieldType getType() {
        return type;
    }

    public boolean isDynamic() {
        return dynamic;
    }
}
