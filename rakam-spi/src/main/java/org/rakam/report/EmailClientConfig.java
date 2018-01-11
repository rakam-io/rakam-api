package org.rakam.report;

import com.google.common.base.Throwables;
import io.airlift.configuration.Config;
import org.rakam.util.MailSender;
import org.rakam.util.RakamException;

import javax.mail.Authenticator;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;

import static io.netty.handler.codec.http.HttpResponseStatus.NOT_IMPLEMENTED;

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

    public String getFromName() {
        return fromName;
    }

    @Config("mail.from-name")
    public void setFromName(String fromName) {
        this.fromName = fromName;
    }

    public URL getSiteUrl() {
        return siteUrl;
    }

    @Config("mail.site-url")
    public void setSiteUrl(URL siteUrl) {
        this.siteUrl = siteUrl;
    }

    public String getHost() {
        return host;
    }

    @Config("mail.smtp.host")
    public void setHost(String host) {
        this.host = host;
    }

    public String getPort() {
        return port;
    }

    @Config("mail.smtp.port")
    public EmailClientConfig setPort(String port) {
        this.port = port;
        return this;
    }

    public String getFromAddress() {
        return fromAddress;
    }

    @Config("mail.from-address")
    public void setFromAddress(String fromAddress) {
        this.fromAddress = fromAddress;
    }

    public String getUser() {
        return user;
    }

    @Config("mail.smtp.user")
    public EmailClientConfig setUser(String user) {
        this.user = user;
        return this;
    }

    public String getPassword() {
        return password;
    }

    @Config("mail.smtp.password")
    public EmailClientConfig setPassword(String password) {
        this.password = password;
        return this;
    }

    public boolean isUseTls() {
        return useTls;
    }

    @Config("mail.smtp.use-tls")
    public void setUseTls(boolean useTls) {
        this.useTls = useTls;
    }

    /*
        The javax documentation doesn't mention but it seems that Session is thread-safe. See http://stackoverflow.com/a/12733317/689144
     */
    public MailSender getMailSender() {
        if (getHost() == null || getUser() == null) {
            throw new RakamException("mail.smtp.host or mail.smtp.username is not set.", NOT_IMPLEMENTED);
        }
        Properties props = new Properties();
        props.put("mail.smtp.auth", "true");
        props.put("mail.smtp.starttls.enable", isUseTls());
        props.put("mail.smtp.host", getHost());
        if (getPort() != null) {
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
