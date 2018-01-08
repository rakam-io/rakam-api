package org.rakam.plugin.user;

import com.fasterxml.jackson.annotation.JsonCreator;
import io.airlift.log.Logger;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.analysis.RequestContext;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.report.EmailClientConfig;
import org.rakam.report.QueryResult;
import org.rakam.server.http.annotations.*;
import org.rakam.util.MailSender;
import org.rakam.util.RakamException;
import org.rakam.util.StringTemplate;

import javax.inject.Inject;
import javax.inject.Named;
import javax.mail.MessagingException;
import javax.mail.internet.AddressException;
import javax.ws.rs.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

@Path("/user/action/email")
@Api(value = "/user/action/email", nickname = "userEmailAction", description = "Email action", tags = "user-action")
public class UserEmailActionService extends UserActionService<UserEmailActionService.EmailActionConfig> {
    private final static Logger LOGGER = Logger.get(UserEmailActionService.class);

    private final MailSender mailSender;
    private final UserHttpService httpService;

    @Inject
    public UserEmailActionService(UserHttpService httpService, EmailClientConfig mailConfig) {
        this.httpService = httpService;

        if (mailConfig.getHost() == null || mailConfig.getUser() == null) {
            throw new IllegalStateException("SMTP configuration is required when mail action is active. See mail.smtp.* configurations.");
        }

        mailSender = mailConfig.getMailSender();
    }

    @JsonRequest
    @ApiOperation(value = "Apply batch operation", authorizations = @Authorization(value = "read_key"))

    @Path("/batch")
    public CompletableFuture<Long> batch(@Named("project") RequestContext context,
                                         @ApiParam(value = "filter", required = false) String filter,
                                         @ApiParam(value = "event_filters", required = false) List<UserStorage.EventFilter> event_filter,
                                         @ApiParam("config") EmailActionConfig config) {
        List<String> variables = new StringTemplate(config.content).getVariables();
        variables.add(config.columnName);

        CompletableFuture<QueryResult> future = httpService.searchUsers(context, variables, filter, event_filter, null, null, 100000);
        return batch(context.project, future, config);
    }

    @Override
    public CompletableFuture<Long> batch(String project, CompletableFuture<QueryResult> queryResult, EmailActionConfig config) {
        StringTemplate template = new StringTemplate(config.content);

        return queryResult.thenApply(result -> {
            Optional<SchemaField> any = result.getMetadata().stream().filter(f -> f.getName().equals(config.columnName)).findAny();
            if (!any.isPresent()) {
                throw new RakamException(String.format("Column %s doesn't exist", config.columnName),
                        HttpResponseStatus.BAD_REQUEST);
            }
            if (any.get().getType() != FieldType.STRING) {
                throw new RakamException("Type of column must be STRING", HttpResponseStatus.BAD_REQUEST);
            }

            final int idx = result.getMetadata().indexOf(any.get());

            Map<String, Integer> colMap = generateColumnMap(template.getVariables(), result.getMetadata());

            long sentEmails = 0;
            for (List<Object> objects : result.getResult()) {
                final String toEmail = (String) objects.get(idx);
                String format = template.format(name -> {
                    Integer index = colMap.get(name);
                    if (index != null) {
                        Object o = objects.get(index);
                        if (o != null && o instanceof String) {
                            return o.toString();
                        }
                    }

                    return config.defaultValues.get(name);
                });

                if (toEmail != null && sendInternal(toEmail, config, format)) {
                    sentEmails++;
                }
            }

            return sentEmails;
        });
    }

    private Map<String, Integer> generateColumnMap(List<String> variables, List<SchemaField> metadata) {
        HashMap<String, Integer> colMap = new HashMap<>(variables.size());

        for (String var : variables) {
            for (int i = 0; i < metadata.size(); i++) {
                if (metadata.get(i).getName().equals(var)) {
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
    @ApiOperation(value = "Perform action for single user", authorizations = @Authorization(value = "read_key"))

    @Path("/single")
    public CompletableFuture<Boolean> send(@Named("project") RequestContext context,
                                           @ApiParam("user") String userId,
                                           @ApiParam("config") EmailActionConfig config) {
        return httpService.getUser(context, userId).thenApply(user -> send(context.project, user, config));
    }

    @Override
    public boolean send(String project, User user, EmailActionConfig config) {
        Object email = user.properties.get(config.columnName);

        if (email != null && email instanceof String) {
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
            mailSender.sendMail(toEmail, config.title, content,
                    config.richText ? Optional.of(content) : Optional.empty(), Stream.empty());
        } catch (AddressException e) {
            return false;
        } catch (MessagingException e) {
            LOGGER.error(e);
            return false;
        }

        return true;
    }

    public static class EmailActionConfig {
        public final String columnName;
        public final String title;
        public final String content;
        public final Map<String, String> defaultValues;
        public final boolean richText;

        @JsonCreator
        public EmailActionConfig(@ApiParam("column_name") String columnName,
                                 @ApiParam("title") String title,
                                 @ApiParam("content") String content,
                                 @ApiParam("variables") Map<String, String> defaultValues,
                                 @ApiParam("rich_text") boolean richText) {
            this.columnName = columnName;
            this.title = title;
            this.content = content;
            this.defaultValues = defaultValues;
            this.richText = richText;
        }
    }
}
