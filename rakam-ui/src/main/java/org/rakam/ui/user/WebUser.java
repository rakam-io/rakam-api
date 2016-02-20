package org.rakam.ui.user;

import org.rakam.collection.event.metastore.Metastore;

import java.util.List;
import java.util.Map;

public class WebUser {
    public final int id;
    public final String email;
    public final String name;
    public final Map<String, List<Metastore.ProjectApiKeys>> projects;

    public WebUser(int id, String email, String name, Map<String, List<Metastore.ProjectApiKeys>> projects) {
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
}
