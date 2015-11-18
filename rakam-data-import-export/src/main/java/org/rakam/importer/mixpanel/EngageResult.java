package org.rakam.importer.mixpanel;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

public class EngageResult {
    public final long page;
    public final long page_size;
    public final List<Person> results;
    public final String session_id;
    public final String status;
    public final long total;

    @JsonCreator
    public EngageResult(@JsonProperty("page") long page,
                        @JsonProperty("page_size") long page_size,
                        @JsonProperty("results") List<Person> results,
                        @JsonProperty("session_id") String session_id,
                        @JsonProperty("status") String status,
                        @JsonProperty("total") long total) {
        this.page = page;
        this.page_size = page_size;
        this.results = results;
        this.session_id = session_id;
        this.status = status;
        this.total = total;
    }

    public static class Person {
        public final String id;
        public final Map<String, Object> properties;

        @JsonCreator
        public Person(@JsonProperty("$distinct_id") String id,
                      @JsonProperty("$properties") Map<String, Object> properties) {
            this.id = id;
            this.properties = properties;
        }
    }
}
