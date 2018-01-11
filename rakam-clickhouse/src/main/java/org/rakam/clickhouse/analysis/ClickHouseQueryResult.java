package org.rakam.clickhouse.analysis;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

class ClickHouseQueryResult {
    public final List<ClickHouseColumn> meta;
    public final List<List<Object>> data;
    public final List<String> totals;
    public final List<Extreme> extremes;
    public final long rows;
    public final Long rowsBeforeLimitAtLeast;

    @JsonCreator
    private ClickHouseQueryResult(
            @JsonProperty("meta") List<ClickHouseColumn> meta,
            @JsonProperty("data") List<List<Object>> data,
            @JsonProperty("totals") List<String> totals,
            @JsonProperty("extremes") List<Extreme> extremes,
            @JsonProperty("rows") long rows,
            @JsonProperty("rows_before_limit_at_least") Long rowsBeforeLimitAtLeast) {
        this.meta = meta;
        this.data = data;
        this.totals = totals;
        this.extremes = extremes;
        this.rows = rows;
        this.rowsBeforeLimitAtLeast = rowsBeforeLimitAtLeast;
    }

    public static class Extreme {
        public final List<String> min;
        public final List<String> max;

        @JsonCreator
        public Extreme(@JsonProperty("min") List<String> min, @JsonProperty("max") List<String> max) {
            this.min = min;
            this.max = max;
        }
    }

    public static class ClickHouseColumn {
        public final String name;
        public final String type;

        @JsonCreator
        public ClickHouseColumn(@JsonProperty("name") String name,
                                @JsonProperty("type") String type) {
            this.name = name;
            this.type = type;
        }
    }
}
