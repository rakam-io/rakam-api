package org.rakam.report;

import com.google.common.base.Throwables;
import io.airlift.configuration.Config;
import org.rakam.util.MailSender;

import javax.mail.Authenticator;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;

public class EmailClientConfig {
    private String host;
    private String port;
    private String user;
    private String password;
    private boolean useTls;
    private String fromAddress = "emre@rakam.io";
    private String fromName = "Rakam.io";
    private URL siteUrl;

    public EmailClientConfig() {
        try {
            siteUrl = new URL("https://app.rakam.io");
        } catch (MalformedURLException e) {
            throw Throwables.propagate(e);
        }
    }

    @Config("mail.smtp.host")
    public void setHost(String host) {
        this.host = host;
    }

    @Config("mail.from-address")
    public void setFromAddress(String fromAddress) {
        this.fromAddress = fromAddress;
    }

    @Config("mail.from-name")
    public void setFromName(String fromName) {
        this.fromName = fromName;
    }

    public String getFromName() {
        return fromName;
    }

    @Config("mail.site-url")
    public void setSiteUrl(URL siteUrl) {
        this.siteUrl = siteUrl;
    }

    public URL getSiteUrl() {
        return siteUrl;
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

    /*
        The javax documentation doesn't mention but it seems that Session is thread-safe. See http://stackoverflow.com/a/12733317/689144
     */
    public MailSender getMailSender() {
        if(getHost() == null || getUser() == null) {
            throw new IllegalStateException("mail.smtp.host or mail.smtp.username is not set.");
        }
        Properties props = new Properties();
        props.put("mail.smtp.auth", "true");
        props.put("mail.smtp.starttls.enable", isUseTls());
        props.put("mail.smtp.host", getHost());
        if(getPort() != null) {
            props.put("mail.smtp.port", getPort());
        }
        Session session = Session.getInstance(props,
                new Authenticator() {
                    protected PasswordAuthentication getPasswordAuthentication() {
                        return new PasswordAuthentication(getUser(), getPassword());
                    }
                });
        return new MailSender(session, getFromAddress(), getFromName());
    }
}
