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

    void deleteProject(String project);

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

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof ProjectApiKeys)) return false;

            ProjectApiKeys that = (ProjectApiKeys) o;

            if (id != that.id) return false;
            if (!project.equals(that.project)) return false;
            if (masterKey != null ? !masterKey.equals(that.masterKey) : that.masterKey != null) return false;
            if (readKey != null ? !readKey.equals(that.readKey) : that.readKey != null) return false;
            return !(writeKey != null ? !writeKey.equals(that.writeKey) : that.writeKey != null);

        }

        @Override
        public int hashCode() {
            int result = id;
            result = 31 * result + project.hashCode();
            result = 31 * result + (masterKey != null ? masterKey.hashCode() : 0);
            result = 31 * result + (readKey != null ? readKey.hashCode() : 0);
            result = 31 * result + (writeKey != null ? writeKey.hashCode() : 0);
            return result;
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