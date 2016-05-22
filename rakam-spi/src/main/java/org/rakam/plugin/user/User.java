package org.rakam.plugin.user;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.rakam.server.http.annotations.ApiParam;

import java.util.Map;


public class User {
    public final Object id;
    public final Map<String, Object> properties;
    public final UserContext api;

    @JsonCreator
    public User(@ApiParam("id") Object id,
                @ApiParam("api") UserContext api,
                @ApiParam("properties") Map<String, Object> properties) {
        this.id = id;
        this.api = api;
        this.properties = properties;
    }

    public static class UserContext {
        public final String apiVersion;
        public final String writeKey;

        @JsonCreator
        public UserContext(@JsonProperty("writeKey") String writeKey,
                           @JsonProperty("apiVersion") String apiVersion) {
            this.writeKey = writeKey;
            this.apiVersion = apiVersion;
        }
    }
}
