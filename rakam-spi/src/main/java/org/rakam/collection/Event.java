package org.rakam.collection;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.avro.generic.GenericRecord;
import org.rakam.server.http.annotations.ApiParam;

@JsonPropertyOrder({ "project", "collection", "context", "properties" })
public class Event {
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final String project;
    private final String collection;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private EventContext context;
    private final GenericRecord properties;

    @JsonCreator
    public Event(@ApiParam("project") String project,
                 @ApiParam("collection") String collection,
                 @ApiParam("api") EventContext context,
                 @ApiParam("properties") GenericRecord properties) {
        this.project = project;
        this.collection = collection;
        this.properties = properties;
        this.context = context;
    }

    @JsonProperty
    public String project() {
        return project;
    }

    @JsonProperty
    public String collection() {
        return collection;
    }

    @JsonProperty
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public EventContext context() {
        return context;
    }

    @JsonProperty
    public GenericRecord properties() {
        return properties;
    }

    public <T> T getAttribute(String attr) {
        return (T) properties().get(attr);
    }


    public static class EventContext {
        public final String writeKey;
        public final String apiVersion;
        public final Long uploadTime;
        public final String checksum;

        @JsonCreator
        public EventContext(@ApiParam(name="writeKey") String writeKey,
                            @ApiParam(name="apiVersion")  String apiVersion,
                            @ApiParam(name="uploadTime") Long uploadTime,
                            @ApiParam(name="checksum") String checksum) {
            this.writeKey = writeKey;
            this.apiVersion = apiVersion;
            this.uploadTime = uploadTime;
            this.checksum = checksum;
        }
    }
}