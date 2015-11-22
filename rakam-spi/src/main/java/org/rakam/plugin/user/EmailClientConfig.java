package org.rakam.plugin.user;

import io.airlift.configuration.Config;

public class EmailClientConfig {
    private String host;
    private String port;
    private String user;
    private String password;
    private boolean useTls;
    private String fromAddress;
    private String fromName;

    @Config("mail.smtp.host")
    public void setHost(String host) {
        this.host = host;
    }

    @Config("mail.from-address")
    public void setFromAddress(String fromAddress) {
        this.fromAddress = fromAddress;
    }

    @Config("mail.from-name")
    public void setFromName(String fromAddress) {
        this.fromName = fromName;
    }

    public String getFromName() {
        return fromName;
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

    @Config("mail.smtp.use-tls")
    public void setUseTls(boolean useTls) {
        this.useTls = useTls;
    }

    public String getHost() {
        return host;
    }

    public String getPort() {
        return port;
    }

    public String getFromAddress() {
        return fromAddress;
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
