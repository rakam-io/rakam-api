package org.rakam.util;

public class MaterializedViewNotExists
        extends NotExistsException {
    private final String itemName;

    public MaterializedViewNotExists(String itemName) {
        super("Referenced materialized table " + itemName);
        this.itemName = itemName;
    }

    public String getTableName() {
        return itemName;
    }
}
