package org.rakam.plugin.user;

import com.fasterxml.jackson.annotation.JsonCreator;
import org.rakam.server.http.annotations.ApiParam;

import java.util.Map;


public class User {
    public final String project;
    public final String id;
    public final Map<String, Object> properties;

    @JsonCreator
    public User(@ApiParam("project") String project,
                @ApiParam("id") String id,
                @ApiParam("properties") Map<String, Object> properties) {
        this.project = project;
        this.id = id;
        this.properties = properties;
    }
}
