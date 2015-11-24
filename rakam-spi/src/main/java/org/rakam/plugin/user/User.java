package org.rakam.plugin.user;

import com.fasterxml.jackson.annotation.JsonCreator;
import org.rakam.plugin.ProjectItem;
import org.rakam.server.http.annotations.ApiParam;

import java.util.Map;


public class User implements ProjectItem {
    public final String project;
    public final String id;
    public final Map<String, Object> properties;

    @JsonCreator
    public User(@ApiParam(name="project") String project,
                @ApiParam(name="id") String id,
                @ApiParam(name="properties") Map<String, Object> properties) {
        this.project = project;
        this.id = id;
        this.properties = properties;
    }

    @Override
    public String project() {
        return project;
    }
}
