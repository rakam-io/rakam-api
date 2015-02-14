package org.rakam.report.metadata.postgresql;

import io.airlift.configuration.Config;

/**
* Created by buremba <Burak Emre Kabakcı> on 11/02/15 01:31.
*/
public class PostgresqlConfig {
    private String database;
    private String username;
    private String password;
    private String host = "127.0.0.1";

    @Config("database")
    public PostgresqlConfig setDatabase(String type)
    {
        this.database = type;
        return this;
    }

    @Config("host")
    public PostgresqlConfig setHost(String host)
    {
        this.host = host;
        return this;
    }

    @Config("username")
    public PostgresqlConfig setUsername(String username)
    {
        this.username = username;
        return this;
    }

    @Config("password")
    public PostgresqlConfig setPassword(String password)
    {
        this.password = password;
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
}
