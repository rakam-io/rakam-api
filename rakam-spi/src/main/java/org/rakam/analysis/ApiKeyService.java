package org.rakam.analysis;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;

import javax.annotation.Nullable;

public interface ApiKeyService {
    ProjectApiKeys createApiKeys(String project);

    String getProjectOfApiKey(String apiKey, AccessKeyType type);

    Key getProjectKey(int apiId, AccessKeyType type);

    class Key {
        public final String project;
        public final String key;

        public Key(String project, String key) {
            this.project = project;
            this.key = key;
        }
    }

    void revokeApiKeys(String project, String masterKey);

    void revokeAllKeys(String project);

    default void setup() {
    }

    enum AccessKeyType {
        MASTER_KEY("master_key"), READ_KEY("read_key"), WRITE_KEY("write_key");

        private final String key;

        AccessKeyType(String key) {
            this.key = key;
        }

        public static AccessKeyType fromKey(String key) {
            for (AccessKeyType accessKeyType : values()) {
                if (accessKeyType.getKey().equals(key)) {
                    return accessKeyType;
                }
            }
            throw new IllegalArgumentException(key + " doesn't exist.");
        }

        public String getKey() {
            return key;
        }
    }

    @AutoValue
    abstract class ProjectApiKeys {
        @JsonCreator
        public static ProjectApiKeys create(
                @JsonProperty("master_key") String masterKey,
                @JsonProperty("read_key") String readKey,
                @JsonProperty("write_key") String writeKey) {
            return new AutoValue_ApiKeyService_ProjectApiKeys(masterKey, readKey, writeKey);
        }

        @Nullable
        @JsonProperty("master_key")
        public abstract String masterKey();

        @Nullable
        @JsonProperty("read_key")
        public abstract String readKey();

        @Nullable
        @JsonProperty("write_key")
        public abstract String writeKey();

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
}
