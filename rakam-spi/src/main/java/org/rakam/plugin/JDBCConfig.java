package org.rakam.plugin;

import io.airlift.configuration.Config;

import java.net.URI;
import java.net.URISyntaxException;


public class JDBCConfig {
    private String url;
    private String table;
    private String username;
    private String password = "";
    private Integer maxConnection;

    @Config("url")
    public JDBCConfig setUrl(String url) throws URISyntaxException {
        if(url.startsWith("jdbc:")) {
            this.url = url;
        } else {
            URI dbUri = new URI(url);
            this.username = dbUri.getUserInfo().split(":")[0];
            this.password = dbUri.getUserInfo().split(":")[1];
            this.url =  "jdbc:"+ convertScheme(dbUri.getScheme()) +"://" + dbUri.getHost() + ':' + dbUri.getPort() + dbUri.getPath() + "?" + dbUri.getQuery();
        }
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

    public String convertScheme(String scheme) {
        switch (scheme) {
            case "postgres":
                return "postgresql";
            default:
                return scheme;
        }
    }

}
