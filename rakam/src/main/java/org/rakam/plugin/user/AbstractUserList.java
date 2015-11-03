package org.rakam.plugin.user;

import io.airlift.log.Logger;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.report.QueryResult;
import org.rakam.util.JsonResponse;

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

public class AbstractUserList {
    final static Logger LOGGER = Logger.get(AbstractUserList.class);

    private final CompletableFuture<QueryResult> queryResult;
    private final EmailClientConfig mailConfig;

    public AbstractUserList(CompletableFuture<QueryResult> queryResult, EmailClientConfig mailConfig) {
        this.queryResult = queryResult;
        this.mailConfig = mailConfig;
    }

    public CompletableFuture<JsonResponse> sendEmail(String columnName, String title, String content, boolean richText) {
        return queryResult.thenApply(result -> {
            Optional<SchemaField> any = result.getMetadata().stream().filter(f -> f.getName().equals(columnName)).findAny();
            if(!any.isPresent()) {
                return JsonResponse.error(String.format("Column %s doesn't exist", columnName));
            }
            if(any.get().getType() != FieldType.STRING) {
                return JsonResponse.error(String.format("Type of email column must be STRING", columnName));
            }

            final int idx = result.getMetadata().indexOf(any.get());

            Properties props = new Properties();
            props.put("mail.smtp.auth", "true");
            props.put("mail.smtp.starttls.enable", mailConfig.isUseTls());
            props.put("mail.smtp.host", mailConfig.getHost());
            props.put("mail.smtp.port", mailConfig.getPort());
            Session session = Session.getInstance(props,
                    new javax.mail.Authenticator() {
                        protected PasswordAuthentication getPasswordAuthentication() {
                            return new PasswordAuthentication(mailConfig.getUser(), mailConfig.getPassword());
                        }
                    });

            int sentEmails = 0;
            for (List<Object> objects : result.getResult()) {
                final String toEmail = (String) objects.get(idx);

                try {
                    Message msg = new MimeMessage(session);
                    msg.setFrom(new InternetAddress("admin@example.com", "Example.com Admin"));
                    msg.addRecipient(Message.RecipientType.TO, new InternetAddress(toEmail));
                    msg.setSubject(title);
                    msg.setText(content);
                    if(richText) {
                        Multipart mp = new MimeMultipart();
                        MimeBodyPart htmlPart = new MimeBodyPart();
                        htmlPart.setContent(columnName, "text/html");
                        mp.addBodyPart(htmlPart);
                        msg.setContent(mp);
                    }
                    Transport.send(msg);
                    sentEmails++;
                } catch (AddressException e) {
                    continue;
                } catch (MessagingException e) {
                    LOGGER.error(e);
                    continue;
                } catch (UnsupportedEncodingException e) {
                    LOGGER.error(e);
                    return JsonResponse.error(String.format("Internal Error", columnName));
                }
            }

            return JsonResponse.success(String.format("Successfully sent %d emails", sentEmails));
        });
    }
}
