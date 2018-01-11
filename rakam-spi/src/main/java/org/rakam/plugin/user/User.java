package org.rakam.plugin.user;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.rakam.collection.Event;
import org.rakam.server.http.annotations.ApiParam;

public class User {
    public final ObjectNode properties;
    public final UserContext api;
    public Object id;

    @JsonCreator
    public User(@ApiParam(value = "id", description = "The value may be a string or a numeric value.") Object id,
                @ApiParam("api") UserContext api,
                @ApiParam("properties") ObjectNode properties) {
        this.id = id;
        this.api = api;
        this.properties = properties;
    }

    public void setId(Object id) {
        this.id = id;
    }

    public static class UserContext {
        @JsonProperty(value = "library")
        public final Event.Library library;
        @JsonProperty(value = "api_key")
        public final String apiKey;
        @JsonProperty(value = "upload_time")
        public final Long uploadTime;
        @JsonProperty(value = "checksum")
        public final String checksum;

        @JsonCreator
        public UserContext(@ApiParam("api_key") String apiKey,
                           @ApiParam(value = "library", required = false) Event.Library library,
                           @ApiParam(value = "api_library", required = false) String apiLibrary,
                           @ApiParam(value = "api_version", required = false) String apiVersion,
                           @ApiParam(value = "upload_time", required = false) Long uploadTime,
                           @ApiParam(value = "checksum", required = false) String checksum) {
            this.apiKey = apiKey;
            this.library = library != null ? library : new Event.Library(apiLibrary, apiVersion);
            this.checksum = checksum;
            this.uploadTime = uploadTime;
        }
    }
}
