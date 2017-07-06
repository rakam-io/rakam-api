package org.rakam.analysis.datasource;

import com.fasterxml.jackson.annotation.JsonInclude;

public class JDBCSchemaConfig
{
    private String username;
    private String password = "";
    private String host;
    private String database;
    private String schema;
    private Integer port;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Boolean enableSSL;

    public JDBCSchemaConfig setUsername(String username)
    {
        this.username = username;
        return this;
    }

    public JDBCSchemaConfig setEnableSSL(boolean enableSSL)
    {
        this.enableSSL = enableSSL;
        return this;
    }

    public Boolean getEnableSSL()
    {
        return enableSSL;
    }

    public JDBCSchemaConfig setSchema(String schema)
    {
        this.schema = schema;
        return this;
    }

    public String getSchema()
    {
        return schema;
    }

    public String getUsername()
    {
        return username;
    }

    public JDBCSchemaConfig setPassword(String password)
    {
        this.password = password;
        return this;
    }

    public String getPassword()
    {
        return password;
    }

    public String getHost()
    {
        return host;
    }

    public Integer getPort()
    {
        return port;
    }

    public JDBCSchemaConfig setPort(Integer port)
    {
        this.port = port;
        return this;
    }

    public JDBCSchemaConfig setHost(String host)
    {
        this.host = host;
        return this;
    }

    public String getDatabase()
    {
        return database;
    }

    public JDBCSchemaConfig setDatabase(String database)
    {
        this.database = database;
        return this;
    }
}
