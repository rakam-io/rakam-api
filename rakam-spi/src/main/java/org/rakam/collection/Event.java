package org.rakam.collection;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.avro.generic.GenericRecord;
import org.rakam.server.http.annotations.ApiParam;

import java.util.List;

@JsonPropertyOrder({ "project", "collection", "api", "properties" })
public class Event {
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final String project;
    private final String collection;
    @JsonIgnore
    private final List<SchemaField> schema;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private EventContext api;
    private final GenericRecord properties;

    @JsonCreator
    public Event(@ApiParam(name="project") String project,
                 @ApiParam(name="collection") String collection,
                 @ApiParam(name="api") EventContext api,
                 @ApiParam(name="properties") GenericRecord properties) {
        this.project = project;
        this.collection = collection;
        this.properties = properties;
        this.api = api;
        this.schema = null;
    }

    public Event(String project,
                 String collection,
                 EventContext api,
                 List<SchemaField> schema,
                 GenericRecord properties) {
        this.project = project;
        this.collection = collection;
        this.properties = properties;
        this.schema = schema;
        this.api = api;
    }

    public Event(String collection,
                 GenericRecord properties) {
        this.collection = collection;
        this.properties = properties;
        this.project = null;
        this.schema = null;
        this.api = null;
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
    public EventContext api() {
        return api;
    }

    @JsonProperty
    public GenericRecord properties() {
        return properties;
    }

    public List<SchemaField> schema() {
        return schema;
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

        @Override
        public String toString() {
            return "EventContext{" +
                    "writeKey='" + writeKey + '\'' +
                    ", apiVersion='" + apiVersion + '\'' +
                    ", uploadTime=" + uploadTime +
                    ", checksum='" + checksum + '\'' +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof EventContext)) return false;

            EventContext context = (EventContext) o;

            if (writeKey != null ? !writeKey.equals(context.writeKey) : context.writeKey != null) return false;
            if (apiVersion != null ? !apiVersion.equals(context.apiVersion) : context.apiVersion != null) return false;
            if (uploadTime != null ? !uploadTime.equals(context.uploadTime) : context.uploadTime != null) return false;
            return !(checksum != null ? !checksum.equals(context.checksum) : context.checksum != null);

        }

        @Override
        public int hashCode() {
            int result = writeKey != null ? writeKey.hashCode() : 0;
            result = 31 * result + (apiVersion != null ? apiVersion.hashCode() : 0);
            result = 31 * result + (uploadTime != null ? uploadTime.hashCode() : 0);
            result = 31 * result + (checksum != null ? checksum.hashCode() : 0);
            return result;
        }
    }
}