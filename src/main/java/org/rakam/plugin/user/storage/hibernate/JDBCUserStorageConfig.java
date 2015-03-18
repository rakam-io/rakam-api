package org.rakam.plugin.user.storage.hibernate;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import io.airlift.configuration.Config;

import java.util.List;

/**
 * Created by buremba <Burak Emre Kabakcı> on 15/03/15 21:27.
 */
public class JDBCUserStorageConfig {
    private List<String> columns;
    private String sessionColumn;
    private String lastSeenColumn;
    private String url;
    private String table;
    private String username;
    private String password = "";

    //    @Config("plugin.user.storage.jdbc.session_column")
//    public void setSessionColumn(String sessionColumn) {
//        this.sessionColumn = sessionColumn;
//    }
//
//    public String getSessionColumn() {
//        return sessionColumn;
//    }
//
//    @Config("plugin.user.storage.jdbc.last_seen_column")
//    public void setLastSeenColumnName(String lastLoginColumnName) {
//        this.lastSeenColumn = lastLoginColumnName;
//    }
//
//    public String getLastSeenColumnName() {
//        return lastSeenColumn;
//    }
//
    @Config("plugin.user.storage.jdbc.columns")
    public void setColumns(String columns) {
        this.columns = ImmutableList.copyOf(Splitter.on(',').omitEmptyStrings().trimResults().split(columns));
    }

    public List<String> getColumns() {
        return columns;
    }

    public String getLastSeenColumn() {
        return lastSeenColumn;
    }

    @Config("plugin.user.storage.jdbc.url")
    public JDBCUserStorageConfig setUrl(String configLocation) {
        this.url = configLocation;
        return this;
    }

    public String getUrl() {
        return url;
    }

    @Config("plugin.user.storage.jdbc.username")
    public JDBCUserStorageConfig setUsername(String username) {
        this.username = username;
        return this;
    }

    public String getUsername() {
        return username;
    }

    public JDBCUserStorageConfig setPassword(String password) {
        this.password = password;
        return this;
    }

    public String getPassword() {
        return password;
    }

    @Config("plugin.user.storage.jdbc.table")
    public JDBCUserStorageConfig setTable(String table) {
        this.table = table;
        return this;
    }

    public String getTable() {
        return table;
    }
}
