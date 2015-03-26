package org.rakam.plugin.user;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Created by buremba <Burak Emre Kabakcı> on 24/03/15 04:33.
 */
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = FilterCriteria.class, name = "filter"),
        @JsonSubTypes.Type(value = ConnectorFilterCriteria.class, name = "filters")
})
public interface FilterClause {
    public boolean isConnector();
}
