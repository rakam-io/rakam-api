package org.rakam.analysis.postgresql;

import io.airlift.configuration.Config;

import java.net.URI;
import java.net.URISyntaxException;

public class PostgresqlConfig {
    private String database = "rakam";
    private String username = "postgres";
    private String password = "";
    private String host = "127.0.0.1";
    private int port = 5432;
    private int maxConnection = 100;

    @Config("store.adapter.postgresql.database")
    public PostgresqlConfig setDatabase(String type)
    {
        this.database = type;
        return this;
    }

    @Config("store.adapter.postgresql.max_connection")
    public PostgresqlConfig setMaxConnection(Integer maxConnection)
    {
        if(maxConnection != null) {
            this.maxConnection = maxConnection;
        }
        return this;
    }

    public int getMaxConnection() {
        return maxConnection;
    }

    @Config("store.adapter.postgresql.url")
    public PostgresqlConfig setUrl(String url) throws URISyntaxException {
        if(url != null && !url.isEmpty()) {
            URI dbUri = new URI(url);
            String userInfo = dbUri.getUserInfo();
            if(userInfo != null) {
                String[] split = userInfo.split(":");
                this.username = split[0];
                if(split.length > 1) {
                    this.password = split[1];
                }

            }
            this.host = dbUri.getHost();
            this.port = dbUri.getPort();
            if(dbUri.getPath() != null && dbUri.getPath().length() > 1) {
                this.database = dbUri.getPath().substring(1);
            }
        }
        return this;
    }

    public String getUrl() throws URISyntaxException {
        return "jdbc:postgresql://" + host + ':' + port + "/" + database;
    }

    @Config("store.adapter.postgresql.host")
    public PostgresqlConfig setHost(String host)
    {
        this.host = host;
        return this;
    }

    @Config("store.adapter.postgresql.username")
    public PostgresqlConfig setUsername(String username)
    {
        this.username = username;
        return this;
    }

    @Config("store.adapter.postgresql.password")
    public PostgresqlConfig setPassword(String password)
    {
        this.password = password;
        return this;
    }

    @Config("store.adapter.postgresql.port")
    public PostgresqlConfig setPort(int port)
    {
        this.port = port;
        return this;
    }

    public String getDatabase() {
        return database;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }
}
