package org.rakam.collection;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.avro.generic.GenericRecord;
import org.rakam.server.http.annotations.ApiParam;


public class Event {
    private final String project;
    private final String collection;
    private final GenericRecord properties;
    private EventContext context;

    @JsonCreator
    public Event(@ApiParam("project") String project,
                 @ApiParam("collection") String collection,
                 @ApiParam("properties") GenericRecord properties,
                 @ApiParam("context") EventContext context) {
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