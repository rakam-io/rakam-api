package org.rakam.plugin.user;

/**
 * Created by buremba <Burak Emre Kabakcı> on 17/03/15 04:30.
 */
public class FilterCriteria {
    public final String attribute;
    public final FilterOperator operator;
    public final Object value;

    public FilterCriteria(String attribute, FilterOperator operator, Object value) {
        this.attribute = attribute;
        this.operator = operator;
        this.value = value;
    }
}
