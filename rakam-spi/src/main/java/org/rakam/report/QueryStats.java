package org.rakam.report;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
* Created by buremba <Burak Emre Kabakcı> on 15/03/15 21:54.
*/
public class QueryStats {
    public final int percentage;
    public final String state;
    public final Integer node;
    public final Long processedRows;
    public final Long processedBytes;
    public final Long userTime;
    public final Long cpuTime;
    public final Long wallTime;

    @JsonCreator
    public QueryStats(@JsonProperty("percentage") int percentage,
                      @JsonProperty("state") String state,
                      @JsonProperty("node") Integer node,
                      @JsonProperty("processedRows") Long processedRows,
                      @JsonProperty("processedBytes") Long processedBytes,
                      @JsonProperty("userTime") Long userTime,
                      @JsonProperty("cpuTime") Long cpuTime,
                      @JsonProperty("wallTime") Long wallTime) {
        this.percentage = percentage;
        this.state = state;
        this.node = node;
        this.processedRows = processedRows;
        this.userTime = userTime;
        this.cpuTime = cpuTime;
        this.wallTime = wallTime;
        this.processedBytes = processedBytes;
    }
}
