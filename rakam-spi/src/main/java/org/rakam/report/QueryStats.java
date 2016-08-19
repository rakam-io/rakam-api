package org.rakam.report;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class QueryStats {
    public final Integer percentage;
    public final State state;
    public final Integer node;
    public final Long processedRows;
    public final Long processedBytes;
    public final Long userTime;
    public final Long cpuTime;
    public final Long wallTime;

    @JsonCreator
    public QueryStats(@JsonProperty("percentage") Integer percentage,
                      @JsonProperty("state") State state,
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

    public QueryStats(State state) {
        this(null, state, null, null, null, null, null, null);
    }

    public enum State {
        /**
         * Query is waiting for available thread and not yet sent to the database.
         */
        WAITING_FOR_AVAILABLE_THREAD(false),
        /**
         * Query has been accepted and is awaiting execution.
         */
        QUEUED(false),
        /**
         * Query is being planned.
         */
        PLANNING(false),
        /**
         * Query execution is being started.
         */
        STARTING(false),
        /**
         * Query has at least one running task.
         */
        RUNNING(false),
        /**
         * Query is finishing (e.g. commit for autocommit queries)
         */
        FINISHING(false),
        /**
         * Query has finished executing and all output has been consumed.
         */
        FINISHED(true),
        /**
         * Query execution failed.
         */
        FAILED(true);

        private final boolean isDone;

        State(boolean isDone) {
            this.isDone = isDone;
        }

        public boolean isDone() {
            return isDone;
        }
    }
}
