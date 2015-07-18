package org.rakam.analysis;

import io.airlift.configuration.Config;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by buremba <Burak Emre Kabakcı> on 03/07/15 04:38.
 */
public class RedshiftConfig {
    private String database;
    private String username;
    private String password = "";
    private String host;
    private int port = 5439;
    private int maxConnection = 15;

    @Config("aws.redshift.database")
    public RedshiftConfig setDatabase(String type)
    {
        this.database = type;
        return this;
    }

    @Config("aws.redshift.max_connection")
    public RedshiftConfig setMaxConnection(Integer maxConnection)
    {
        if(maxConnection != null) {
            this.maxConnection = maxConnection;
        }
        return this;
    }

    public int getMaxConnection() {
        return maxConnection;
    }

    @Config("aws.redshift.url")
    public RedshiftConfig setUrl(String url) throws URISyntaxException {
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

    public String getUrl() {
        return "jdbc:postgresql://" + host + ':' + port + "/" + database;
    }

    @Config("aws.redshift.host")
    public RedshiftConfig setHost(String host)
    {
        this.host = host;
        return this;
    }

    @Config("aws.redshift.username")
    public RedshiftConfig setUsername(String username)
    {
        this.username = username;
        return this;
    }

    @Config("aws.redshift.password")
    public RedshiftConfig setPassword(String password)
    {
        this.password = password;
        return this;
    }

    @Config("aws.redshift.port")
    public RedshiftConfig setPort(int port)
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
