package org.rakam.analysis.postgresql;

import io.airlift.configuration.Config;

/**
* Created by buremba <Burak Emre Kabakcı> on 11/02/15 01:31.
*/
public class PostgresqlConfig {
    private String database;
    private String username;
    private String password;
    private String host = "127.0.0.1";
    private int port = 5432;

    @Config("store.adapter.postgresql.database")
    public PostgresqlConfig setDatabase(String type)
    {
        this.database = type;
        return this;
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
