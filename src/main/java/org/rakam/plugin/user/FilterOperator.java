package org.rakam.plugin.user;

/**
 * Created by buremba <Burak Emre Kabakcı> on 17/03/15 04:31.
 */
public enum FilterOperator {
    $lte(true),
    $lt(true),
    $gt(true),
    $gte(true),
    $contains(true),
    $eq(true),
    $in(true),
    $regex(true),
    $is_null(false),
    $is_set(false),
    $starts_with(true);

    private final boolean requiresValue;

    FilterOperator(boolean requiresValue) {
        this.requiresValue = requiresValue;
    }

    public boolean requiresValue() {
        return requiresValue;
    }
}
