package org.rakam.report;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
* Created by buremba <Burak Emre Kabakcı> on 15/03/15 21:54.
*/
public class QueryStats {
    public final int percentage;
    public final String state;
    public final int node;
    public final long processedRows;
    public final long processedBytes;
    public final long userTime;
    public final long cpuTime;
    public final long wallTime;

    @JsonCreator
    public QueryStats(@JsonProperty("percentage") int percentage,
                      @JsonProperty("state") String state,
                      @JsonProperty("node") int node,
                      @JsonProperty("processedRows") long processedRows,
                      @JsonProperty("processedBytes") long processedBytes,
                      @JsonProperty("userTime") long userTime,
                      @JsonProperty("cpuTime") long cpuTime,
                      @JsonProperty("wallTime") long wallTime) {
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
