package org.rakam.plugin.user.mailbox.jdbc;

import io.airlift.configuration.Config;

/**
 * Created by buremba <Burak Emre Kabakcı> on 30/03/15 16:52.
 */
public class JDBCUserMailboxConfig {
    private String url;
    private String table;
    private String username;
    private String password = "";

    @Config("url")
    public JDBCUserMailboxConfig setUrl(String configLocation) {
        this.url = configLocation;
        return this;
    }

    public String getUrl() {
        return url;
    }

    @Config("username")
    public JDBCUserMailboxConfig setUsername(String username) {
        this.username = username;
        return this;
    }

    public String getUsername() {
        return username;
    }

    @Config("password")
    public JDBCUserMailboxConfig setPassword(String password) {
        this.password = password;
        return this;
    }

    public String getPassword() {
        return password;
    }

    @Config("table")
    public JDBCUserMailboxConfig setTable(String table) {
        this.table = table;
        return this;
    }

    public String getTable() {
        return table;
    }
}
