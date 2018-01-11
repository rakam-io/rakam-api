package org.rakam.collection;

import com.fasterxml.jackson.annotation.*;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.rakam.server.http.annotations.ApiParam;

import java.util.List;

@JsonPropertyOrder({"project", "collection", "api", "properties"})
public class Event {
    @JsonIgnore
    private final String project;
    private final String collection;
    @JsonIgnore
    private List<SchemaField> schema;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private EventContext api;
    private GenericRecord properties;

    @JsonCreator
    public Event(@ApiParam(value = "collection", description = "The collection of event (pageview, touch, click etc.)") String collection,
                 @ApiParam("api") EventContext api,
                 @ApiParam(value = "properties", description = "The properties of the event") GenericRecord properties) {
        this(null, collection, api, null, properties);
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

    @Override
    public String toString() {
        return "Event{" +
                "project='" + project + '\'' +
                ", collection='" + collection + '\'' +
                ", schema=" + schema +
                ", api=" + api +
                ", properties=" + properties +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Event)) {
            return false;
        }

        Event event = (Event) o;

        if (!project.equals(event.project)) {
            return false;
        }
        if (!collection.equals(event.collection)) {
            return false;
        }
        if (schema != null ? !schema.equals(event.schema) : event.schema != null) {
            return false;
        }
        if (api != null ? !api.equals(event.api) : event.api != null) {
            return false;
        }
        return properties.equals(event.properties);
    }

    @Override
    public int hashCode() {
        int result = project.hashCode();
        result = 31 * result + collection.hashCode();
        result = 31 * result + (schema != null ? schema.hashCode() : 0);
        result = 31 * result + (api != null ? api.hashCode() : 0);
        result = 31 * result + properties.hashCode();
        return result;
    }

    // TODO: find a way to make this class immutable
    public void properties(GenericData.Record record, List<SchemaField> fields) {
        properties = record;
        schema = fields;
    }

    public static class Library {
        public final String name;
        public final String version;

        @JsonCreator
        public Library(@ApiParam(value = "name", required = false) String name, @ApiParam(value = "version", required = false) String version) {
            this.name = name;
            this.version = version;
        }
    }

    public static class EventContext {
        private static final EventContext EMPTY_CONTEXT = new EventContext(null, null, null, null, null, null);

        @JsonProperty("api_key")
        public final String apiKey;
        @JsonProperty(value = "library")
        public final Library library;
        @JsonProperty(value = "api_version")
        public final String apiVersion;
        @JsonProperty(value = "upload_time")
        public final Long uploadTime;
        @JsonProperty(value = "checksum")
        public final String checksum;
        @JsonProperty(value = "uuid")
        public final String uuid;

        @JsonCreator
        public EventContext(
                @ApiParam(value = "api_key", required = false) String apiKey,
                @ApiParam(value = "library", required = false, description = "Optional library information for statistics") Library library,
                @ApiParam(value = "api_version", required = false, description = "Optional API version for versioning") String apiVersion,
                @ApiParam(value = "upload_time", required = false, description = "Optional client upload time for clock synchronization") Long uploadTime,
                @ApiParam(value = "uuid", required = false, description = "Optional UUID for deduplication") String uuid,
                @ApiParam(value = "checksum", required = false, description = "Optional checksum for verify the body content") String checksum) {
            this.library = library;
            this.apiKey = apiKey;
            this.apiVersion = apiVersion;
            this.uploadTime = uploadTime;
            this.uuid = uuid;
            this.checksum = checksum;
        }

        public static EventContext apiKey(String apiKey) {
            return new EventContext(apiKey, null, null, null, null, null);
        }

        public static EventContext empty() {
            return EMPTY_CONTEXT;
        }

        @Override
        public String toString() {
            return "EventContext{" +
                    "apiKey='" + apiKey + '\'' +
                    ", apiVersion='" + apiVersion + '\'' +
                    ", uploadTime=" + uploadTime +
                    ", checksum='" + checksum + '\'' +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof EventContext)) {
                return false;
            }

            EventContext context = (EventContext) o;

            if (apiKey != null ? !apiKey.equals(context.apiKey) : context.apiKey != null) {
                return false;
            }
            if (apiVersion != null ? !apiVersion.equals(context.apiVersion) : context.apiVersion != null) {
                return false;
            }
            if (uploadTime != null ? !uploadTime.equals(context.uploadTime) : context.uploadTime != null) {
                return false;
            }
            return !(checksum != null ? !checksum.equals(context.checksum) : context.checksum != null);
        }

        @Override
        public int hashCode() {
            int result = apiKey != null ? apiKey.hashCode() : 0;
            result = 31 * result + (apiVersion != null ? apiVersion.hashCode() : 0);
            result = 31 * result + (uploadTime != null ? uploadTime.hashCode() : 0);
            result = 31 * result + (checksum != null ? checksum.hashCode() : 0);
            return result;
        }
    }
}