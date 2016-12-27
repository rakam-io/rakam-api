package org.rakam.ui.user;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.rakam.analysis.ApiKeyService.ProjectApiKeys;

import java.time.ZoneId;
import java.util.List;

public class WebUser {
    public final int id;
    public final String email;
    public final String name;
    public final boolean readOnly;
    public final List<Project> projects;

    public WebUser(int id, String email, String name, boolean readOnly, List<Project> projects) {
        this.id = id;
        this.email = email;
        this.name = name;
        this.readOnly = readOnly;
        this.projects = projects;
    }

    public static class UserApiKey {
        public final int project;
        @JsonProperty("read_key") public final String readKey;
        @JsonProperty("write_key") public final String writeKey;
        @JsonProperty("master_key") public final String masterKey;

        public UserApiKey(int project, String readKey, String writeKey, String masterKey) {
            this.project = project;
            this.readKey = readKey;
            this.writeKey = writeKey;
            this.masterKey = masterKey;
        }
    }

    public static class Project {
        public final int id;
        public final String name;
        public final String apiUrl;
        public final ZoneId timezone;
        public final List<ProjectApiKeys> apiKeys;

        public Project(int id, String name, String apiUrl, ZoneId zoneId, List<ProjectApiKeys> apiKeys) {
            this.id = id;
            this.name = name;
            this.timezone = zoneId;
            this.apiKeys = apiKeys;
            this.apiUrl = apiUrl;
        }

        @Override
        public String toString()
        {
            return "Project{" +
                    "id=" + id +
                    ", name='" + name + '\'' +
                    ", apiUrl='" + apiUrl + '\'' +
                    ", timezone=" + timezone +
                    ", apiKeys=" + apiKeys +
                    '}';
        }
    }
}
