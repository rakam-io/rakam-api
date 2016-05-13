package org.rakam.ui.user;

import org.rakam.analysis.ApiKeyService.ProjectApiKeys;

import java.util.List;

public class WebUser {
    public final int id;
    public final String email;
    public final String name;
    public final List<Project> projects;

    public WebUser(int id, String email, String name, List<Project> projects) {
        this.id = id;
        this.email = email;
        this.name = name;
        this.projects = projects;
    }

    public static class UserApiKey {
        public final String readKey;
        public final String writeKey;
        public final String masterKey;

        public UserApiKey(String readKey, String writeKey, String masterKey) {
            this.readKey = readKey;
            this.writeKey = writeKey;
            this.masterKey = masterKey;
        }
    }

    public static class Project {
        public final String name;
        public final String apiUrl;
        public final List<ProjectApiKeys> apiKeys;

        public Project(String name, String apiUrl, List<ProjectApiKeys> apiKeys) {
            this.name = name;
            this.apiKeys = apiKeys;
            this.apiUrl = apiUrl;
        }
    }
}
