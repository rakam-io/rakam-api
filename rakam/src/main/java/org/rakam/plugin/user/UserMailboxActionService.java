package org.rakam.plugin.user;

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Expression;
import com.fasterxml.jackson.annotation.JsonCreator;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.collection.SchemaField;
import org.rakam.plugin.UserStorage;
import org.rakam.plugin.user.mailbox.Message;
import org.rakam.plugin.user.mailbox.UserMailboxStorage;
import org.rakam.report.QueryResult;
import org.rakam.server.http.annotations.Api;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.server.http.annotations.ApiResponse;
import org.rakam.server.http.annotations.ApiResponses;
import org.rakam.server.http.annotations.Authorization;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.util.RakamException;
import org.rakam.util.StringTemplate;

import javax.inject.Inject;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static java.lang.String.format;

@Path("/user/action/mailbox")
@Api(value = "/user/action/mailbox", description = "Mailbox action", tags = "user-action")
public class UserMailboxActionService extends UserActionService<UserMailboxActionService.MailAction> {
    private final SqlParser sqlParser = new SqlParser();
    private final AbstractUserService userService;
    private final UserMailboxStorage mailboxStorage;

    @Inject
    public UserMailboxActionService(AbstractUserService userService, UserMailboxStorage mailboxStorage) {
        this.userService = userService;
        this.mailboxStorage = mailboxStorage;
    }

    @JsonRequest
    @ApiOperation(value = "Apply batch operation")
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Project does not exist.")})
    @Path("/batch")
    public CompletableFuture<Long> batchSendMessages(@ApiParam(name="project") String project,
                                                     @ApiParam(name = "filter", required = false) String filter,
                                                     @ApiParam(name = "event_filters", required = false) List<UserStorage.EventFilter> event_filter,
                                                     @ApiParam(name = "config") MailAction config) {
        List<String> variables = new StringTemplate(config.message).getVariables();
        variables.add(UserStorage.PRIMARY_KEY);

        Expression expression;
        if (filter != null) {
            try {
                synchronized (sqlParser) {
                    expression = sqlParser.createExpression(filter);
                }
            } catch (Exception e) {
                throw new RakamException(format("filter expression '%s' couldn't parsed", filter),
                        HttpResponseStatus.BAD_REQUEST);
            }
        } else {
            expression = null;
        }

        CompletableFuture<QueryResult> future = userService.filter(project, variables, expression, event_filter, null, 100000, null);
        return batch(project, future, config);
    }


    @Override
    public String getName() {
        return "mailbox";
    }

    @Override
    public boolean send(User user, MailAction config) {
        mailboxStorage.send(user.project, config.fromUser, user.id, null, config.message, Instant.now());
        return true;
    }

    public static class MailAction {
        public final String fromUser;
        public final String message;
        public final Map<String, String> variables;

        @JsonCreator
        public MailAction(@ApiParam(name="from_user") String fromUser,
                          @ApiParam(name="message") String message,
                          @ApiParam(name="variables") Map<String, String> variables) {
            this.fromUser = fromUser;
            this.message = message;
            this.variables = variables;
        }
    }

    @Override
    public CompletableFuture<Long> batch(String project, CompletableFuture<QueryResult> queryResult, MailAction action) {
        StringTemplate template = new StringTemplate(action.message);
        return queryResult.thenApply(result -> {
            List<SchemaField> metadata = result.getMetadata();
            int key = metadata.indexOf(metadata.stream().filter(a -> a.getName().equals(UserStorage.PRIMARY_KEY)).findAny().get());
            Map<String, Integer> map = generateColumnMap(template.getVariables(), metadata);

            for (List<Object> objects : result.getResult()) {
                final String userId = objects.get(key).toString();
                String format = template.format(name -> {
                    Integer index = map.get(name);
                    if(index != null) {
                        Object o = objects.get(index);
                        if (o != null && o instanceof String) {
                            return o.toString();
                        }
                    }

                    return action.variables.get(name);
                });

                mailboxStorage.send(project, action.fromUser, userId, null, format, Instant.now());
            }

            return (long) result.getResult().size();
        });
    }

    @Path("/action/mailbox/single")
    @POST
    @JsonRequest
    @ApiOperation(value = "Send message to user",
            notes = "Sends a mail to users mailbox",
            authorizations = @Authorization(value = "write_key")
    )
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Project does not exist."),
            @ApiResponse(code = 404, message = "User does not exist.")})
    public Message send(@ApiParam(name = "project", value = "Project id", required = true) String project,
                        @ApiParam(name = "from_user", required = true) String fromUser,
                        @ApiParam(name = "to_user", required = true) String toUser,
                        @ApiParam(name = "parent", value = "Parent message id", required = false) Integer parent,
                        @ApiParam(name = "message", value = "The content of the message", required = false) String message,
                        @ApiParam(name = "timestamp", value = "The timestamp of the message", required = true) long datetime) {
        try {
            return mailboxStorage.send(project, fromUser, toUser, parent, message, Instant.ofEpochMilli(datetime));
        } catch (Exception e) {
            throw new RakamException("Error while sending message: "+e.getMessage(), HttpResponseStatus.BAD_REQUEST);
        }
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

}
