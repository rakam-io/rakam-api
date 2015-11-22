package org.rakam.plugin.user;

import io.airlift.log.Logger;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.report.QueryResult;
import org.rakam.util.RakamException;

import javax.inject.Inject;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

public class UserEmailActionService implements UserActionService<UserEmailActionService.EmailActionConfig> {
    final static Logger LOGGER = Logger.get(UserEmailActionService.class);

    private final EmailClientConfig mailConfig;
    private final Session session;

    @Inject
    public UserEmailActionService(EmailClientConfig mailConfig) {
        this.mailConfig = mailConfig;

        if(mailConfig.getHost() == null || mailConfig.getUser() == null) {
            throw new IllegalStateException("SMTP configuration is required when mail action is active. See mail.smtp.* configurations.");
        }

        Properties props = new Properties();
        props.put("mail.smtp.auth", "true");
        props.put("mail.smtp.starttls.enable", mailConfig.isUseTls());
        props.put("mail.smtp.host", mailConfig.getHost());
        props.put("mail.smtp.port", mailConfig.getPort());
        session = Session.getInstance(props,
                new javax.mail.Authenticator() {
                    protected PasswordAuthentication getPasswordAuthentication() {
                        return new PasswordAuthentication(mailConfig.getUser(), mailConfig.getPassword());
                    }
                });
    }

    public static class EmailActionConfig {
        public final String columnName;
        public final String title;
        public final String content;
        public final boolean richTest;

        public EmailActionConfig(String columnName, String title, String content, boolean richTest) {
            this.columnName = columnName;
            this.title = title;
            this.content = content;
            this.richTest = richTest;
        }
    }

    @Override
    public CompletableFuture<Long> batch(CompletableFuture<QueryResult> queryResult, EmailActionConfig config) {
        return queryResult.thenApply(result -> {
            Optional<SchemaField> any = result.getMetadata().stream().filter(f -> f.getName().equals(config.columnName)).findAny();
            if(!any.isPresent()) {
                throw new RakamException(String.format("Column %s doesn't exist", config.columnName),
                        HttpResponseStatus.BAD_REQUEST);
            }
            if(any.get().getType() != FieldType.STRING) {
                throw new RakamException("Type of column must be STRING", HttpResponseStatus.BAD_REQUEST);
            }

            final int idx = result.getMetadata().indexOf(any.get());

            long sentEmails = 0;
            for (List<Object> objects : result.getResult()) {
                final String toEmail = (String) objects.get(idx);
                if(toEmail != null) {
                    if(sendInternal(toEmail, config)) {
                        sentEmails++;
                    }
                }
            }

            return sentEmails;
        });
    }

    @Override
    public CompletableFuture<Boolean> send(User user, EmailActionConfig config) {

        Object email = user.properties.get(config.columnName);
        if(email != null && email instanceof String) {
            return CompletableFuture.supplyAsync(() -> sendInternal((String) email, config));
        } else {
            return CompletableFuture.completedFuture(false);
        }
    }

    @Override
    public String getActionName() {
        return "email";
    }

    private boolean sendInternal(String toEmail, EmailActionConfig config) {
        try {
            Message msg = new MimeMessage(session);
            msg.setFrom(new InternetAddress(mailConfig.getFromAddress(), mailConfig.getFromName()));
            msg.addRecipient(Message.RecipientType.TO, new InternetAddress(toEmail));
            msg.setSubject(config.title);
            msg.setText(config.title);
            if(config.richTest) {
                Multipart mp = new MimeMultipart();
                MimeBodyPart htmlPart = new MimeBodyPart();
                htmlPart.setContent(toEmail, "text/html");
                mp.addBodyPart(htmlPart);
                msg.setContent(mp);
            }
            Transport.send(msg);
        } catch (AddressException e) {
            return false;
        }  catch (UnsupportedEncodingException|MessagingException e) {
            LOGGER.error(e);
            return false;
        }

        return true;
    }
}
