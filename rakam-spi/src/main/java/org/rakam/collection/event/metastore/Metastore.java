package org.rakam.collection.event.metastore;

import org.rakam.analysis.ProjectNotExistsException;
import org.rakam.collection.SchemaField;

import java.util.List;
import java.util.Map;
import java.util.Set;


public interface Metastore {
    Map<String, Set<String>> getAllCollections();

    Map<String, List<SchemaField>> getCollections(String project);

    Set<String> getCollectionNames(String project);

    ProjectApiKeyList createApiKeys(String project);

    void createProject(String project);

    Set<String> getProjects();

    List<SchemaField> getCollection(String project, String collection);

    List<SchemaField> getOrCreateCollectionFieldList(String project, String collection, List<SchemaField> fields) throws ProjectNotExistsException;

    boolean checkPermission(String project, AccessKeyType type, String apiKey);

    class ProjectApiKeyList {
        public final String masterKey;
        public final String readKey;
        public final String writeKey;

        public ProjectApiKeyList(String masterKey, String readKey, String writeKey) {
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