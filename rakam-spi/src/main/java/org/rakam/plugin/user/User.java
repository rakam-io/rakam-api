package org.rakam.plugin.user;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.rakam.plugin.ProjectItem;
import org.rakam.server.http.annotations.ApiParam;

import java.util.Map;


public class User implements ProjectItem {
    public final String project;
    public final String id;
    public final Map<String, Object> properties;
    public final UserContext api;

    @JsonCreator
    public User(@ApiParam(name="project") String project,
                @ApiParam(name="id") String id,
                @ApiParam(name="api") UserContext api,
                @ApiParam(name="properties") Map<String, Object> properties) {
        this.project = project;
        this.id = id;
        this.api = api;
        this.properties = properties;
    }

    @Override
    public String project() {
        return project;
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
