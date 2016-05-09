package org.rakam.analysis;

import java.util.List;

public interface ApiKeyService {
    ProjectApiKeys createApiKeys(String project);

    String getProjectOfApiKey(String apiKey, AccessKeyType type);

    void revokeApiKeys(String project, int id);

    boolean checkPermission(String project, AccessKeyType type, String apiKey);

    List<ProjectApiKeys> getApiKeys(int[] ids);

    void revokeAllKeys(String project);

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

        public String getKey(AccessKeyType accessKeyType) {
            switch (accessKeyType) {
                case WRITE_KEY:
                    return writeKey;
                case MASTER_KEY:
                    return masterKey;
                case READ_KEY:
                    return readKey;
                default:
                    throw new IllegalStateException();
            }
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

        public static AccessKeyType fromKey(String key) {
            for (AccessKeyType accessKeyType : values()) {
                if (accessKeyType.getKey().equals(key)) {
                    return accessKeyType;
                }
            }
            throw new IllegalArgumentException(key + " doesn't exist.");
        }
    }
}
