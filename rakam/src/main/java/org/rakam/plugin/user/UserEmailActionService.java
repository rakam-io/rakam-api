package org.rakam.plugin.user;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.log.Logger;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.report.QueryResult;
import org.rakam.server.http.annotations.Api;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.server.http.annotations.ApiResponse;
import org.rakam.server.http.annotations.ApiResponses;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.util.RakamException;
import org.rakam.util.StringTemplate;

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
import javax.ws.rs.Path;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

@Path("/user/action/email")
@Api(value = "/user/action/email", description = "Email action", tags = "user-action")
public class UserEmailActionService extends UserActionService<UserEmailActionService.EmailActionConfig> {
    final static Logger LOGGER = Logger.get(UserEmailActionService.class);

    private final EmailClientConfig mailConfig;
    private final Session session;
    private final UserHttpService httpService;

    @Inject
    public UserEmailActionService(UserHttpService httpService, EmailClientConfig mailConfig) {
        this.httpService = httpService;
        this.mailConfig = mailConfig;

        if(mailConfig.getHost() == null || mailConfig.getUser() == null) {
            throw new IllegalStateException("SMTP configuration is required when mail action is active. See mail.smtp.* configurations.");
        }

        Properties props = new Properties();
        props.put("mail.smtp.auth", "true");
        props.put("mail.smtp.starttls.enable", mailConfig.isUseTls());
        props.put("mail.smtp.host", mailConfig.getHost());
        if(mailConfig.getPort() != null) {
            props.put("mail.smtp.port", mailConfig.getPort());
        }
        session = Session.getInstance(props,
                new javax.mail.Authenticator() {
                    protected PasswordAuthentication getPasswordAuthentication() {
                        return new PasswordAuthentication(mailConfig.getUser(), mailConfig.getPassword());
                    }
                });
    }

    @JsonRequest
    @ApiOperation(value = "Apply batch operation")
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Project does not exist.")})
    @Path("/batch")
    public CompletableFuture<Long> batchSendEmails(@ApiParam(name="project") String project,
                                         @ApiParam(name = "filter", required = false) String filter,
                                         @ApiParam(name = "event_filters", required = false) List<UserStorage.EventFilter> event_filter,
                                         @ApiParam(name = "config") EmailActionConfig config) {
        List<String> variables = new StringTemplate(config.content).getVariables();
        variables.add(config.columnName);

        CompletableFuture<QueryResult> future = httpService.searchUsers(project, variables, filter, event_filter, null, null, 100000);
        return batch(project, future, config);
    }

    public static class EmailActionConfig {
        public final String columnName;
        public final String title;
        public final String content;
        public final Map<String, String> defaultValues;
        public final boolean richText;

        @JsonCreator
        public EmailActionConfig(@JsonProperty("column_name") String columnName,
                                 @JsonProperty("title") String title,
                                 @JsonProperty("content") String content,
                                 @JsonProperty("variables") Map<String, String> defaultValues,
                                 @JsonProperty("rich_text") boolean richText) {
            this.columnName = columnName;
            this.title = title;
            this.content = content;
            this.defaultValues = defaultValues;
            this.richText = richText;
        }
    }

    @Override
    public CompletableFuture<Long> batch(String project, CompletableFuture<QueryResult> queryResult, EmailActionConfig config) {
        StringTemplate template = new StringTemplate(config.content);

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

            Map<String, Integer> colMap = generateColumnMap(template.getVariables(), result.getMetadata());

            long sentEmails = 0;
            for (List<Object> objects : result.getResult()) {
                final String toEmail = (String) objects.get(idx);
                String format = template.format(name -> {
                    Integer index = colMap.get(name);
                    if(index != null) {
                        Object o = objects.get(index);
                        if (o != null && o instanceof String) {
                            return o.toString();
                        }
                    }

                    return config.defaultValues.get(name);
                });

                if(toEmail != null) {
                    if(sendInternal(toEmail, config, format)) {
                        sentEmails++;
                    }
                }
            }

            return sentEmails;
        });
    }

    private Map<String, Integer> generateColumnMap(List<String> variables, List<SchemaField> metadata) {
        HashMap<String, Integer> colMap = new HashMap<>(variables.size());

        for (String var : variables) {
            for (int i = 0; i < metadata.size(); i++) {
                if(metadata.get(i).getName().equals(var)) {
                    colMap.put(variables.get(i), i);
                    break;
                }
            }
        }

        return colMap;
    }

    @Override
    public String getName() {
        return "email";
    }


    @JsonRequest
    @ApiOperation(value = "Perform action for single user")
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Project does not exist.")})
    @Path("/single")
    public CompletableFuture<Boolean> send(@ApiParam(name="project") String project,
                                           @ApiParam(name="user") String userId,
                                           @ApiParam(name="config") EmailActionConfig config) {
        return httpService.getUser(project, userId).thenApply(user -> send(user, config));
    }

    @Override
    public boolean send(User user, EmailActionConfig config) {
        Object email = user.properties.get(config.columnName);

        if(email != null && email instanceof String) {
                StringTemplate template = new StringTemplate(config.content);

                String format = template.format(name -> {
                    Object o = user.properties.get(name);
                    if (o != null && o instanceof String) {
                        return o.toString();
                    } else {
                        return config.defaultValues.get(name);
                    }
                });

                return sendInternal((String) email, config, format);
        } else {
            return false;
        }
    }

    private boolean sendInternal(String toEmail, EmailActionConfig config, String content) {
        try {
            Message msg = new MimeMessage(session);
            msg.setFrom(new InternetAddress(mailConfig.getFromAddress(), mailConfig.getFromName()));
            msg.addRecipient(Message.RecipientType.TO, new InternetAddress(toEmail));
            msg.setSubject(config.title);
            msg.setText(content);
            if(config.richText) {
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
