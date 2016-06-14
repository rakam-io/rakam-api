package org.rakam.plugin.user;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.rakam.collection.Event;
import org.rakam.server.http.annotations.ApiParam;

import java.util.Map;

public class User
{
    public Object id;
    public final Map<String, Object> properties;
    public final UserContext api;

    @JsonCreator
    public User(@ApiParam("id") Object id,
            @ApiParam("api") UserContext api,
            @ApiParam("properties") Map<String, Object> properties)
    {
        this.id = id;
        this.api = api;
        this.properties = properties;
    }

    public void setId(Object id)
    {
        this.id = id;
    }

    public static class UserContext
    {
        @JsonProperty("library") public final Event.Library library;
        @JsonProperty("api_key") public final String apiKey;
        @JsonProperty("upload_time") public final Long uploadTime;
        @JsonProperty("checksum") public final String checksum;

        @JsonCreator
        public UserContext(@ApiParam("api_key") String apiKey,
                @ApiParam(value = "writeKey", access = "internal") String writeKey,
                @ApiParam(value = "library") Event.Library library,
                @ApiParam(value = "api_library", access = "internal") String apiLibrary,
                @ApiParam(value = "api_version", access = "internal") String apiVersion,
                @ApiParam("upload_time") Long uploadTime,
                @ApiParam("checksum") String checksum)
        {
            this.apiKey = apiKey != null ? apiKey : writeKey;
            this.library = library != null ? library : new Event.Library(apiLibrary, apiVersion);
            this.checksum = checksum;
            this.uploadTime = uploadTime;
        }
    }
}
