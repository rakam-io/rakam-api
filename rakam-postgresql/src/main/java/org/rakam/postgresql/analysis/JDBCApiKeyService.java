package org.rakam.postgresql.analysis;

import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.analysis.ApiKeyService;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.util.CryptUtil;
import org.rakam.util.RakamException;

import javax.annotation.PostConstruct;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.rakam.analysis.ApiKeyService.AccessKeyType.*;

public class JDBCApiKeyService implements ApiKeyService {
    private final LoadingCache<String, List<Set<String>>> apiKeyCache;
    private final JDBCPoolDataSource connectionPool;
    private final LoadingCache<ApiKey, String> apiKeyReverseCache;

    public JDBCApiKeyService(JDBCPoolDataSource connectionPool) {
        this.connectionPool = connectionPool;

        apiKeyCache = CacheBuilder.newBuilder().build(new CacheLoader<String, List<Set<String>>>() {
            @Override
            public List<Set<String>> load(String project) throws Exception {
                try (Connection conn = connectionPool.getConnection()) {
                    return getKeys(conn, project);
                }
            }
        });

        apiKeyReverseCache = CacheBuilder.newBuilder().build(new CacheLoader<ApiKey, String>() {
            @Override
            public String load(ApiKey apiKey) throws Exception {
                try (Connection conn = connectionPool.getConnection()) {
                    PreparedStatement ps = conn.prepareStatement(format("SELECT project FROM api_key WHERE %s = ?", apiKey.type.name()));
                    ps.setString(1, apiKey.key);
                    ResultSet resultSet = ps.executeQuery();
                    if (!resultSet.next()) {
                        throw new RakamException("API key is invalid", HttpResponseStatus.FORBIDDEN);
                    }
                    return resultSet.getString(1);
                } catch (SQLException e) {
                    throw Throwables.propagate(e);
                }
            }
        });
    }

    @PostConstruct
    public void setup() {
        try (Connection connection = connectionPool.getConnection()) {
            Statement statement = connection.createStatement();
            statement.execute("CREATE TABLE IF NOT EXISTS api_key (" +
                    "  id MEDIUMINT NOT NULL AUTO_INCREMENT,\n" +
                    "  project VARCHAR(255) NOT NULL,\n" +
                    "  read_key VARCHAR(255) NOT NULL,\n" +
                    "  write_key VARCHAR(255) NOT NULL,\n" +
                    "  master_key VARCHAR(255) NOT NULL,\n" +
                    "  created_at TIMESTAMP default current_timestamp NOT NULL," +
                    "PRIMARY KEY (id)\n" +
                    "  )");
        } catch (SQLException e) {
            Throwables.propagate(e);
        }
    }

    @Override
    public ProjectApiKeys createApiKeys(String project) {

        String masterKey = CryptUtil.generateRandomKey(64);
        String readKey = CryptUtil.generateRandomKey(64);
        String writeKey = CryptUtil.generateRandomKey(64);

        int id;
        try (Connection connection = connectionPool.getConnection()) {
            PreparedStatement ps = connection.prepareStatement("INSERT INTO api_key " +
                            "(master_key, read_key, write_key, project) VALUES (?, ?, ?, ?)",
                    Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, masterKey);
            ps.setString(2, readKey);
            ps.setString(3, writeKey);
            ps.setString(4, project);
            ps.executeUpdate();
            final ResultSet generatedKeys = ps.getGeneratedKeys();
            generatedKeys.next();
            id = generatedKeys.getInt(1);
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }

        return new ProjectApiKeys(id, project, masterKey, readKey, writeKey);
    }

    @Override
    public String getProjectOfApiKey(String apiKey, AccessKeyType type) {
        try {
            return apiKeyReverseCache.getUnchecked(new ApiKey(apiKey, type));
        } catch (UncheckedExecutionException e) {
            throw Throwables.propagate(e.getCause());
        }
    }

    @Override
    public void revokeApiKeys(String project, int id) {
        try (Connection conn = connectionPool.getConnection()) {
            PreparedStatement ps = conn.prepareStatement("DELETE FROM api_key WHERE project = ? AND id = ?");
            ps.setString(1, project);
            ps.setInt(2, id);
            ps.execute();
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public boolean checkPermission(String project, AccessKeyType type, String apiKey) {
        try {
            if (apiKey == null) {
                throw new RakamException("Api key is missing", HttpResponseStatus.FORBIDDEN);
            }
            if (project == null) {
                throw new RakamException("Project id is missing", HttpResponseStatus.FORBIDDEN);
            }
            boolean exists = apiKeyCache.get(project).get(type.ordinal()).contains(apiKey);
            if (!exists) {
                apiKeyCache.refresh(project);
                return apiKeyCache.get(project).get(type.ordinal()).contains(apiKey);
            }
            return true;
        } catch (ExecutionException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public List<ProjectApiKeys> getApiKeys(int[] ids) {
        try (Connection conn = connectionPool.getConnection()) {

            final PreparedStatement ps = conn.prepareStatement(String.format("select id, project, master_key, read_key, write_key from api_key where id in (%s)",
                    Arrays.stream(ids).mapToObj(i -> "?").collect(Collectors.joining(", "))));
            for (int i = 0; i < ids.length; i++) {
                ps.setInt(i + 1, ids[i]);
            }
            ps.execute();
            final ResultSet resultSet = ps.getResultSet();
            final List<ProjectApiKeys> list = Lists.newArrayList();
            while (resultSet.next()) {
                list.add(new ProjectApiKeys(resultSet.getInt(1), resultSet.getString(2), resultSet.getString(3), resultSet.getString(4), resultSet.getString(5)));
            }
            return Collections.unmodifiableList(list);
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void revokeAllKeys(String project) {
        try (Connection conn = connectionPool.getConnection()) {
            PreparedStatement ps = conn.prepareStatement("DELETE FROM api_key WHERE project = ?");
            ps.setString(1, project);
            ps.execute();
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    private List<Set<String>> getKeys(Connection conn, String project) throws SQLException {
        Set<String> masterKeyList = new HashSet<>();
        Set<String> readKeyList = new HashSet<>();
        Set<String> writeKeyList = new HashSet<>();

        Set<String>[] keys =
                Arrays.stream(AccessKeyType.values()).map(key -> new HashSet<String>()).toArray(Set[]::new);

        PreparedStatement ps = conn.prepareStatement("SELECT master_key, read_key, write_key from api_key WHERE project = ?");
        ps.setString(1, project);
        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()) {
            String apiKey;

            apiKey = resultSet.getString(1);
            if (apiKey != null) {
                masterKeyList.add(apiKey);
            }
            apiKey = resultSet.getString(2);
            if (apiKey != null) {
                readKeyList.add(apiKey);
            }
            apiKey = resultSet.getString(3);
            if (apiKey != null) {
                writeKeyList.add(apiKey);
            }
        }

        keys[MASTER_KEY.ordinal()] = Collections.unmodifiableSet(masterKeyList);
        keys[READ_KEY.ordinal()] = Collections.unmodifiableSet(readKeyList);
        keys[WRITE_KEY.ordinal()] = Collections.unmodifiableSet(writeKeyList);

        return Collections.unmodifiableList(Arrays.asList(keys));
    }

    public void clearCache() {
        apiKeyCache.cleanUp();
    }

    public static final class ApiKey {
        public final String key;
        public final AccessKeyType type;

        public ApiKey(String key, AccessKeyType type) {
            this.key = key;
            this.type = type;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof ApiKey)) return false;

            ApiKey apiKey = (ApiKey) o;

            if (!key.equals(apiKey.key)) return false;
            return type == apiKey.type;

        }

        @Override
        public int hashCode() {
            int result = key.hashCode();
            result = 31 * result + type.hashCode();
            return result;
        }
    }
}
