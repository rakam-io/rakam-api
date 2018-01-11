package org.rakam.util;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

import javax.mail.*;
import javax.mail.internet.*;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

public class MailSender {
    private final Session session;
    private final InternetAddress fromAddress;

    public MailSender(Session session, String fromAddress, String fromName) {
        this.session = session;
        try {
            this.fromAddress = new InternetAddress(fromAddress, fromName);
        } catch (UnsupportedEncodingException e) {
            throw Throwables.propagate(e);
        }
    }

    public void sendMail(String toEmail, String title, String textContent, Optional<String> richText, Stream<MimeBodyPart> parts)
            throws MessagingException {
        sendMail(ImmutableList.of(toEmail), title, textContent, richText, parts);
    }

    public void sendMail(List<String> toEmail, String title, String textContent, Optional<String> richText, Stream<MimeBodyPart> parts)
            throws MessagingException {
        Message msg = new MimeMessage(session);
        msg.setFrom(fromAddress);
        msg.addRecipients(MimeMessage.RecipientType.TO, toEmail.stream().map(e -> {
            try {
                return new InternetAddress(e);
            } catch (AddressException e1) {
                throw Throwables.propagate(e1);
            }
        }).toArray(InternetAddress[]::new));
        msg.setSubject(title);
        if (textContent != null) {
            msg.setText(textContent);
        }

        if (richText.isPresent()) {
            Multipart mp = new MimeMultipart();
            MimeBodyPart htmlPart = new MimeBodyPart();
            htmlPart.setContent(richText.get(), "text/html");
            mp.addBodyPart(htmlPart);
            parts.forEach(part -> {
                try {
                    mp.addBodyPart(part);
                } catch (MessagingException e) {
                    throw Throwables.propagate(e);
                }
            });
            msg.setContent(mp);
        }
        Transport.send(msg);
    }
}