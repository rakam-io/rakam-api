package org.rakam.util;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import java.io.UnsupportedEncodingException;
import java.util.Optional;

import static javax.mail.Message.RecipientType.TO;

public class MailSender {
    private final Session session;
    private final String fromAddress;
    private final String fromName;

    public MailSender(Session session, String fromAddress, String fromName) {
        this.session = session;
        this.fromAddress = fromAddress;
        this.fromName = fromName;
    }

    public void sendMail(String toEmail, String title, String textContent, Optional<String> richText)
            throws MessagingException, UnsupportedEncodingException {
        Message msg = new MimeMessage(session);
        msg.setFrom(new InternetAddress(fromAddress, fromName));
        msg.addRecipient(TO, new InternetAddress(toEmail));
        msg.setSubject(title);
        msg.setText(textContent);
        if(richText.isPresent()) {
            Multipart mp = new MimeMultipart();
            MimeBodyPart htmlPart = new MimeBodyPart();
            htmlPart.setContent(richText.get(), "text/html");
            mp.addBodyPart(htmlPart);
            msg.setContent(mp);
        }
        Transport.send(msg);
    }
}
