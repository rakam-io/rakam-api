package org.rakam.analysis;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;

public interface ApiKeyService {
    ProjectApiKeys createApiKeys(String project);

    String getProjectOfApiKey(String apiKey, AccessKeyType type);

    void revokeApiKeys(String project, String masterKey);

    boolean checkPermission(String project, AccessKeyType type, String apiKey);

    void revokeAllKeys(String project);

    @AutoValue
    abstract class ProjectApiKeys {
        @JsonProperty public abstract String masterKey();
        @JsonProperty public abstract String readKey();
        @JsonProperty public abstract String writeKey();

        @JsonCreator
        public static ProjectApiKeys create(@JsonProperty("masterKey") String masterKey,
                                     @JsonProperty("readKey") String readKey,
                                     @JsonProperty("writeKey") String writeKey) {
            return new AutoValue_ApiKeyService_ProjectApiKeys(masterKey, readKey, writeKey);
        }

        public String getKey(AccessKeyType accessKeyType) {
            switch (accessKeyType) {
                case WRITE_KEY:
                    return writeKey();
                case MASTER_KEY:
                    return masterKey();
                case READ_KEY:
                    return readKey();
                default:
                    throw new IllegalStateException();
            }
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
