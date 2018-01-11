package org.rakam.report;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class QuerySampling {
    public final SampleMethod method;
    public final int percentage;

    @JsonCreator
    public QuerySampling(
            @JsonProperty("method") SampleMethod method,
            @JsonProperty("percentage") int percentage) {
        this.method = method;
        this.percentage = percentage;
    }

    public enum SampleMethod {
        BERNOULLI, SYSTEM;

        @JsonCreator
        public static SampleMethod get(String name) {
            return valueOf(name.toUpperCase());
        }

        @JsonProperty
        public String value() {
            return name();
        }
    }
}
