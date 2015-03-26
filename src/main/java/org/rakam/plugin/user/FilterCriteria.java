package org.rakam.plugin.user;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotNull;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Created by buremba <Burak Emre Kabakcı> on 17/03/15 04:30.
 */
public class FilterCriteria implements FilterClause{
    @NotNull
    private final String attribute;
    @NotNull
    private final FilterOperator operator;
    private final Object value;

    @JsonCreator
    public FilterCriteria(@JsonProperty("attribute") String attribute,
                          @JsonProperty("operator") FilterOperator operator,
                          @JsonProperty("value") Object value) {
        this.attribute = checkNotNull(attribute, "attribute is null");
        this.operator = checkNotNull(operator, "type is null");
        this.value = value;
    }

    public String getAttribute() {
        return attribute;
    }

    public FilterOperator getOperator() {
        return operator;
    }

    public Object getValue() {
        return value;
    }

    @Override
    public boolean isConnector() {
        return false;
    }
}
