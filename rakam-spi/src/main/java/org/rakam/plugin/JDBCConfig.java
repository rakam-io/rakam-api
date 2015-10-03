package org.rakam.plugin;

import io.airlift.configuration.Config;


public class JDBCConfig {
    private String url;
    private String table;
    private String username;
    private String password = "";
    private Integer maxConnection;

    @Config("url")
    public JDBCConfig setUrl(String configLocation) {
        this.url = configLocation;
        return this;
    }

    public String getUrl() {
        return url;
    }

    @Config("username")
    public JDBCConfig setUsername(String username) {
        this.username = username;
        return this;
    }

    public String getUsername() {
        return username;
    }

    @Config("max_connection")
    public JDBCConfig setMaxConnection(Integer maxConnection) {
        this.maxConnection = maxConnection;
        return this;
    }

    public Integer getMaxConnection() {
        return maxConnection;
    }

    @Config("password")
    public JDBCConfig setPassword(String password) {
        this.password = password;
        return this;
    }

    public String getPassword() {
        return password;
    }

    @Config("table")
    public JDBCConfig setTable(String table) {
        this.table = table;
        return this;
    }

    public String getTable() {
        return table;
    }
}
