package org.rakam.plugin.user;

import io.airlift.configuration.Config;

public class EmailClientConfig {
    private String host;
    private String port;
    private String user;
    private String password;
    private boolean useTls;

    @Config("mail.smtp.host")
    public void setHost(String host) {
        this.host = host;
    }

    @Config("mail.smtp.port")
    public void setPort(String port) {
        this.port = port;
    }

    @Config("mail.smtp.user")
    public void setUser(String user) {
        this.user = user;
    }

    @Config("mail.smtp.password")
    public void setPassword(String password) {
        this.password = password;
    }

    @Config("mail.smtp.use-tsl")
    public void setUseTls(boolean useTls) {
        this.useTls = useTls;
    }

    public String getHost() {
        return host;
    }

    public String getPort() {
        return port;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public boolean isUseTls() {
        return useTls;
    }
}
