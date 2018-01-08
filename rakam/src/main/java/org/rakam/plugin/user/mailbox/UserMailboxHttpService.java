package org.rakam.plugin.user.mailbox;

import com.google.common.collect.ImmutableMap;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.analysis.ApiKeyService;
import org.rakam.analysis.RequestContext;
import org.rakam.plugin.user.UserPluginConfig;
import org.rakam.server.http.HttpServer;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.server.http.annotations.*;
import org.rakam.util.SuccessMessage;

import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.firstNonNull;
import static org.rakam.util.JsonHelper.encode;

@Path("/user/mailbox")
@Api(value = "/user/mailbox", nickname = "userMailbox", description = "UserMailbox", tags = "user-mailbox")
public class UserMailboxHttpService extends HttpService {
    private final UserMailboxStorage storage;
    private final MailBoxWebSocketService webSocketService;
    private final UserPluginConfig config;
    private final ApiKeyService apiKeyService;

    @Inject
    public UserMailboxHttpService(ApiKeyService apiKeyService, UserPluginConfig config, UserMailboxStorage storage, MailBoxWebSocketService webSocketService) {
        this.storage = storage;
        this.config = config;
        this.apiKeyService = apiKeyService;
        this.webSocketService = webSocketService;
    }

    @Path("/get")
    @JsonRequest
    @ApiOperation(value = "Get user mailbox",
            notes = "Returns the last mails sent to the user",
            response = Message.class,
            responseContainer = "List",
            authorizations = @Authorization(value = "read_key")
    )
    @ApiResponses(value = {@ApiResponse(code = 404, message = "User does not exist.")})
    public List<Message> getMailbox(@Named("project") RequestContext context,
                                    @ApiParam(value = "user", description = "User id") String user,
                                    @ApiParam(value = "parent", description = "Parent message id", required = false) Integer parent,
                                    @ApiParam(value = "limit", description = "Message query result limit", allowableValues = "range[1,100]", required = false) Integer limit,
                                    @ApiParam(value = "offset", description = "Message query result offset", required = false) Long offset) {
        return storage.getConversation(context.project, user, parent, firstNonNull(limit, 100), firstNonNull(offset, 0L));
    }

    @Path("/listen")
    @GET
    @ApiImplicitParams({
            @ApiImplicitParam(name = "project", dataType = "string", paramType = "query"),
    })
    @ApiOperation(value = "Listen all mailboxes",
            consumes = "text/event-stream",
            produces = "text/event-stream",
            authorizations = @Authorization(value = "read_key")
    )
    @IgnoreApi
    public void listenMails(RakamHttpRequest request) {
        RakamHttpRequest.StreamResponse response = request.streamResponse();

        List<String> apiKey = request.params().get("api_key");
        if (apiKey == null || apiKey.isEmpty()) {
            response.send("result", encode(HttpServer.errorMessage("api_key query parameter is required", HttpResponseStatus.BAD_REQUEST))).end();
            return;
        }

        List<String> api_key = request.params().get("read_key");
        String project = apiKeyService.getProjectOfApiKey(api_key.get(0), ApiKeyService.AccessKeyType.READ_KEY);


        UserMailboxStorage.MessageListener update = storage.listenAllUsers(project,
                data -> response.send("update", data.serialize()));

        response.listenClose(() -> update.shutdown());
    }

    @JsonRequest
    @ApiOperation(value = "Mark mail as read",
            notes = "Marks the specified mails as read.",
            authorizations = @Authorization(value = "write_key")
    )
    @ApiResponses(value = {@ApiResponse(code = 404, message = "Message does not exist."),
            @ApiResponse(code = 404, message = "User does not exist.")})
    @Path("/mark_as_read")
    public SuccessMessage markAsRead(
            @Named("project") RequestContext context,
            @ApiParam(value = "user", description = "User id") String user,
            @ApiParam(value = "message_ids", description = "The list of of message ids that will be marked as read") int[] message_ids) {
        storage.markMessagesAsRead(context.project, user, message_ids);
        return SuccessMessage.success();
    }

    @Path("/get_online_users")
    @POST
    @JsonRequest
    @ApiOperation(value = "Get connected users",
            authorizations = @Authorization(value = "read_key")
    )

    public CompletableFuture<Collection<Map<String, Object>>> getConnectedUsers(@Named("project") RequestContext context) {
        Collection<Object> connectedUsers = webSocketService.getConnectedUsers(context.project);
        return CompletableFuture.completedFuture(connectedUsers.stream()
                .map(id -> ImmutableMap.of(config.getIdentifierColumn(), id))
                .collect(Collectors.toList()));
    }


}
