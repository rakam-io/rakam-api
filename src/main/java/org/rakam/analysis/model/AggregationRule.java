package org.rakam.analysis.model;

import org.rakam.constant.Analysis;

import java.io.Serializable;
import java.util.HashMap;
import java.util.UUID;

/**
 * Created by buremba on 16/01/14.
 */
public abstract class AggregationRule implements Serializable {
    public final UUID id;
    public String groupBy = null;
    public final HashMap<String, String> filters;

    public AggregationRule(UUID id) {
        this.id = id;
        this.filters = null;
    }

    public AggregationRule(UUID id, HashMap<String, String> filters) {
        this.id = id;
        this.filters = filters;
    }

    public AggregationRule(UUID id, HashMap<String, String> filters, String groupBy) {
        this(id, filters);
        this.groupBy = groupBy;
    }

    @Override
    public String toString() {
        return "UUID {" + id + '}';
    }

    public abstract Analysis getType();
}
