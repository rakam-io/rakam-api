package org.rakam.plugin.user;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotNull;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Created by buremba <Burak Emre Kabakcı> on 17/03/15 04:30.
 */
public class FilterCriteria {
    @NotNull
    private final String attribute;
    @NotNull
    private final FilterOperator type;
    private final Object value;

    @JsonCreator
    public FilterCriteria(@JsonProperty("attribute") String attribute,
                          @JsonProperty("type") FilterOperator type,
                          @JsonProperty("value") Object value) {
        this.attribute = checkNotNull(attribute, "attribute is null");
        this.type = checkNotNull(type, "type is null");
        this.value = value;
    }

    public String getAttribute() {
        return attribute;
    }

    public FilterOperator getType() {
        return type;
    }

    public Object getValue() {
        return value;
    }
}
