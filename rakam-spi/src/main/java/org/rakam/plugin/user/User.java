package org.rakam.plugin.user;

import com.fasterxml.jackson.annotation.JsonCreator;
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
        public final String apiLibrary;
        public final String apiVersion;
        public final String apiKey;

        @JsonCreator
        public UserContext(@ApiParam("api_key") String apiKey,
                           @ApiParam(value = "writeKey", access = "internal") String writeKey,
                           @ApiParam("api_library") String apiLibrary,
                           @ApiParam("api_version") String apiVersion) {
            this.apiKey = apiKey != null ? apiKey : writeKey;
            this.apiLibrary = apiLibrary;
            this.apiVersion = apiVersion;
        }
    }
}
