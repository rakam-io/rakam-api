package org.rakam.collection.event.metastore;

import org.rakam.analysis.NotExistsException;
import org.rakam.collection.SchemaField;

import java.util.List;
import java.util.Map;
import java.util.Set;


public interface Metastore {
    Map<String, Set<String>> getAllCollections();

    Map<String, List<SchemaField>> getCollections(String project);

    Set<String> getCollectionNames(String project);

    ProjectApiKeys createApiKeys(String project);

    void revokeApiKeys(String project, int id);

    void createProject(String project);

    Set<String> getProjects();

    List<SchemaField> getCollection(String project, String collection);

    List<SchemaField> getOrCreateCollectionFieldList(String project, String collection, Set<SchemaField> fields) throws NotExistsException;

    boolean checkPermission(String project, AccessKeyType type, String apiKey);

    List<ProjectApiKeys> getApiKeys(int[] ids);

    class ProjectApiKeys {
        public final int id;
        public final String project;
        public final String masterKey;
        public final String readKey;
        public final String writeKey;

        public ProjectApiKeys(int id, String project, String masterKey, String readKey, String writeKey) {
            this.id = id;
            this.project = project;
            this.masterKey = masterKey;
            this.readKey = readKey;
            this.writeKey = writeKey;
        }
    }

    enum AccessKeyType {
        MASTER_KEY("master_key"), READ_KEY("read_key"), WRITE_KEY("write_key");

        private final String key;

        AccessKeyType(String key) {
            this.key = key;
        }

        public String getKey() {
            return key;
        }
    }
}