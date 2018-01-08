package org.rakam.config;

import io.airlift.configuration.Config;

import javax.validation.constraints.NotNull;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;

public class JDBCConfig {
    private String url;
    private String table;
    private String username;
    private String password = "";
    private Integer maxConnection;
    private String dataSource;
    private Long connectionMaxLifeTime;
    private Long connectionIdleTimeout;
    private boolean connectionDisablePool;

    @NotNull
    public String getUrl() {
        return url;
    }

    @Config("url")
    public JDBCConfig setUrl(String url)
            throws URISyntaxException {
        if (url.startsWith("jdbc:")) {
            url = url.substring(5);
        }

        URI dbUri = new URI(url);
        String userInfo = dbUri.getUserInfo();
        if (userInfo != null) {
            String[] split = userInfo.split(":");
            this.username = split[0];
            if (split.length > 1) {
                this.password = split[1];
            }
        }

        String query = Optional.ofNullable(dbUri.getQuery()).orElse("");
        if (dbUri.getScheme().equals("postgresql")) {
            if (!query.isEmpty()) {
                query += "&";
            }
            query += "currentSchema=public";
        }

        this.url = "jdbc:" + convertScheme(dbUri.getScheme()) + ":" +
                (dbUri.getHost() != null ? "//" + dbUri.getHost() : "") +
                ((dbUri.getHost() != null && dbUri.getPort() > -1) ? (":" + dbUri.getPort()) : "")
                + dbUri.getPath()
                + (query.isEmpty() ? "" : ("?" + query));

        return this;
    }

    public String getDataSource() {
        return dataSource;
    }

    @Config("data-source")
    public JDBCConfig setDataSource(String dataSource) {
        this.dataSource = dataSource;
        return this;
    }

    public String getUsername() {
        return username;
    }

    @Config("username")
    public JDBCConfig setUsername(String username) {
        this.username = username;
        return this;
    }

    public Integer getMaxConnection() {
        return maxConnection;
    }

    @Config("max-connection")
    public JDBCConfig setMaxConnection(Integer maxConnection) {
        this.maxConnection = maxConnection;
        return this;
    }

    public String getPassword() {
        return password;
    }

    @Config("password")
    public JDBCConfig setPassword(String password) {
        this.password = password;
        return this;
    }

    public boolean getConnectionDisablePool() {
        return connectionDisablePool;
    }

    @Config("connection.disable-pool")
    public JDBCConfig setConnectionDisablePool(boolean connectionDisablePool) {
        this.connectionDisablePool = connectionDisablePool;
        return this;
    }

    public String getTable() {
        return table;
    }

    @Config("table")
    public JDBCConfig setTable(String table) {
        this.table = table;
        return this;
    }

    public Long getConnectionMaxLifeTime() {
        return connectionMaxLifeTime;
    }

    @Config("connection.max-life-time")
    public JDBCConfig setConnectionMaxLifeTime(Long connectionMaxLifeTime) {
        this.connectionMaxLifeTime = connectionMaxLifeTime;
        return this;
    }

    public Long getConnectionIdleTimeout() {
        return connectionIdleTimeout;
    }

    @Config("connection.max-idle-timeout")
    public JDBCConfig setConnectionIdleTimeout(Long connectionIdleTimeout) {
        this.connectionIdleTimeout = connectionIdleTimeout;
        return this;
    }

    public String convertScheme(String scheme) {
        switch (scheme) {
            case "postgres":
                return "postgresql";
            default:
                return scheme;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof JDBCConfig)) {
            return false;
        }

        JDBCConfig that = (JDBCConfig) o;

        if (url != null ? !url.equals(that.url) : that.url != null) {
            return false;
        }
        if (table != null ? !table.equals(that.table) : that.table != null) {
            return false;
        }
        if (username != null ? !username.equals(that.username) : that.username != null) {
            return false;
        }
        if (password != null ? !password.equals(that.password) : that.password != null) {
            return false;
        }
        if (maxConnection != null ? !maxConnection.equals(that.maxConnection) : that.maxConnection != null) {
            return false;
        }
        return !(dataSource != null ? !dataSource.equals(that.dataSource) : that.dataSource != null);
    }

    @Override
    public int hashCode() {
        int result = url != null ? url.hashCode() : 0;
        result = 31 * result + (table != null ? table.hashCode() : 0);
        result = 31 * result + (username != null ? username.hashCode() : 0);
        result = 31 * result + (password != null ? password.hashCode() : 0);
        result = 31 * result + (maxConnection != null ? maxConnection.hashCode() : 0);
        result = 31 * result + (dataSource != null ? dataSource.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return username + "@" + url;
    }
}
