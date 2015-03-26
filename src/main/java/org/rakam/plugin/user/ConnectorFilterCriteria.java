package org.rakam.plugin.user;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;

/**
 * Created by buremba <Burak Emre Kabakcı> on 24/03/15 04:40.
 */
public class ConnectorFilterCriteria implements FilterClause {
    List<FilterClause> filterClauseList;
    Connector connector;

    @JsonCreator
    public ConnectorFilterCriteria(@JsonProperty("filters") List<FilterClause> filters,
                                   @JsonProperty("connector") Connector connector) {
        this.filterClauseList = Collections.unmodifiableList(filters);
        this.connector = connector;
    }

    @JsonProperty("filters")
    public List<FilterClause> getClauses() {
        return filterClauseList;
    }

    @JsonProperty("connector")
    public Connector getConnector() {
        return connector;
    }

    @Override
    public boolean isConnector() {
        return true;
    }

    public static enum Connector {
        OR, AND
    }
}
