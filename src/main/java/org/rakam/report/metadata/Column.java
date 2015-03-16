package org.rakam.report.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
* Created by buremba <Burak Emre Kabakcı> on 15/03/15 21:55.
*/
public class Column {
    public final String name;
    public final String type;
    public final int position;

    @JsonCreator
    public Column(@JsonProperty("name") String name,
                  @JsonProperty("type") String type,
                  @JsonProperty("position") int position) {
        this.name = name;
        this.type = type;
        this.position = position;
    }

    @JsonProperty
    public String getName() {
        return name;
    }

    @JsonProperty
    public String getType() {
        return type;
    }
}
