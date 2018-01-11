package org.rakam.plugin.user.mailbox;

import com.facebook.presto.sql.tree.Expression;
import com.fasterxml.jackson.annotation.JsonCreator;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.analysis.RequestContext;
import org.rakam.collection.SchemaField;
import org.rakam.plugin.user.AbstractUserService;
import org.rakam.plugin.user.User;
import org.rakam.plugin.user.UserActionService;
import org.rakam.plugin.user.UserStorage;
import org.rakam.plugin.user.UserStorage.EventFilter;
import org.rakam.report.QueryResult;
import org.rakam.server.http.annotations.*;
import org.rakam.util.RakamException;
import org.rakam.util.StringTemplate;

import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.rakam.plugin.user.UserHttpService.parseExpression;

@Path("/user/action/mailbox")
@Api(value = "/user/action/mailbox", nickname = "userMailboxAction", description = "Mailbox action", tags = "user-action")
public class UserMailboxActionService extends UserActionService<UserMailboxActionService.MailAction> {
    private final AbstractUserService userService;
    private final UserMailboxStorage mailboxStorage;

    @Inject
    public UserMailboxActionService(AbstractUserService userService, UserMailboxStorage mailboxStorage) {
        this.userService = userService;
        this.mailboxStorage = mailboxStorage;
    }

    @JsonRequest
    @ApiOperation(value = "Apply batch operation", authorizations = @Authorization(value = "read_key"))

    @Path("/batch")
    public CompletableFuture<Long> batchSendMessages(@Named("project") RequestContext context,
                                                     @ApiParam(value = "filter", required = false) String filter,
                                                     @ApiParam(value = "event_filters", required = false) List<EventFilter> event_filter,
                                                     @ApiParam("config") MailAction config) {
        List<String> variables = new StringTemplate(config.message).getVariables();
        variables.add(UserStorage.PRIMARY_KEY);

        Expression expression = parseExpression(filter);

        CompletableFuture<QueryResult> future = userService.searchUsers(context, variables, expression, event_filter, null, 100000, null);
        return batch(context.project, future, config);
    }


    @Override
    public String getName() {
        return "mailbox";
    }

    @Override
    public boolean send(String project, User user, MailAction config) {
        mailboxStorage.send(project, config.fromUser, user.id, null, config.message, Instant.now());
        return true;
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
                    if (index != null) {
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
    @ApiResponses(value = {@ApiResponse(code = 404, message = "User does not exist.")})
    public Message sendMail(@Named("project") RequestContext context,
                            @ApiParam("from_user") String fromUser,
                            @ApiParam("to_user") String toUser,
                            @ApiParam(value = "parent", description = "Parent message id", required = false) Integer parent,
                            @ApiParam(value = "message", description = "The content of the message", required = false) String message,
                            @ApiParam(value = "timestamp", description = "The timestamp of the message") long datetime) {
        try {
            return mailboxStorage.send(context.project, fromUser, toUser, parent, message, Instant.ofEpochMilli(datetime));
        } catch (Exception e) {
            throw new RakamException("Error while sending message: " + e.getMessage(), HttpResponseStatus.BAD_REQUEST);
        }
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

    public static class MailAction {
        public final String fromUser;
        public final String message;
        public final Map<String, String> variables;

        @JsonCreator
        public MailAction(@ApiParam("from_user") String fromUser,
                          @ApiParam("message") String message,
                          @ApiParam("variables") Map<String, String> variables) {
            this.fromUser = fromUser;
            this.message = message;
            this.variables = variables;
        }
    }

}
