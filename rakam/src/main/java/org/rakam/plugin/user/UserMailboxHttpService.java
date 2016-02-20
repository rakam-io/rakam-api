package org.rakam.plugin.user;

import com.google.common.collect.ImmutableMap;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.util.IgnorePermissionCheck;
import org.rakam.collection.event.metastore.Metastore;
import org.rakam.plugin.user.mailbox.Message;
import org.rakam.plugin.user.mailbox.UserMailboxStorage;
import org.rakam.server.http.HttpServer;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.server.http.annotations.Api;
import org.rakam.server.http.annotations.ApiImplicitParam;
import org.rakam.server.http.annotations.ApiImplicitParams;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.server.http.annotations.ApiResponse;
import org.rakam.server.http.annotations.ApiResponses;
import org.rakam.server.http.annotations.Authorization;
import org.rakam.server.http.annotations.IgnoreApi;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.util.JsonResponse;

import javax.inject.Inject;
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
@Api(value = "/user/mailbox", description = "UserMailbox", tags = {"user-mailbox"})
public class UserMailboxHttpService extends HttpService {
    private final UserMailboxStorage storage;
    private final MailBoxWebSocketService webSocketService;
    private final UserPluginConfig config;
    private final Metastore metastore;

    @Inject
    public UserMailboxHttpService(Metastore metastore, UserPluginConfig config, UserMailboxStorage storage, MailBoxWebSocketService webSocketService) {
        this.storage = storage;
        this.config = config;
        this.metastore = metastore;
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
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Project does not exist."),
            @ApiResponse(code = 404, message = "User does not exist.")})
    public List<Message> get(@ApiParam(name = "project", value = "Project id", required = true) String project,
                             @ApiParam(name = "user", value = "User id", required = true) String user,
                             @ApiParam(name = "parent", value = "Parent message id", required = false) Integer parent,
                             @ApiParam(name = "limit", value = "Message query result limit", allowableValues = "range[1,100]", required = false) Integer limit,
                             @ApiParam(name = "offset", value = "Message query result offset", required = false) Long offset) {
        return storage.getConversation(project, user, parent, firstNonNull(limit, 100), firstNonNull(offset, 0L));
    }

    @Path("/listen")
    @GET
    @ApiImplicitParams({
            @ApiImplicitParam(name = "project", required = true, dataType = "string", paramType = "query"),
    })
    @ApiOperation(value = "Listen all mailboxes",
            consumes = "text/event-stream",
            produces = "text/event-stream",
            authorizations = @Authorization(value = "read_key")
    )
    @IgnoreApi
    @IgnorePermissionCheck
    public void listen(RakamHttpRequest request) {
        RakamHttpRequest.StreamResponse response = request.streamResponse();

        List<String> project = request.params().get("project");
        if(project == null || project.isEmpty()) {
            response.send("result", encode(HttpServer.errorMessage("project query parameter is required", HttpResponseStatus.BAD_REQUEST))).end();
            return;
        }

        List<String> api_key = request.params().get("api_key");
        if(api_key == null || api_key.isEmpty() || !metastore.checkPermission(project.get(0), Metastore.AccessKeyType.READ_KEY, api_key.get(0))) {
            response.send("result", encode(HttpServer.errorMessage(HttpResponseStatus.UNAUTHORIZED.reasonPhrase(),
                    HttpResponseStatus.UNAUTHORIZED))).end();
            return;
        }

        UserMailboxStorage.MessageListener update = storage.listenAllUsers(project.get(0),
                data -> response.send("update", data.serialize()));

        response.listenClose(() -> update.shutdown());
    }

    @JsonRequest
    @ApiOperation(value = "Mark mail as read",
            notes = "Marks the specified mails as read.",
            authorizations = @Authorization(value = "write_key")
    )
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Project does not exist."),
            @ApiResponse(code = 404, message = "Message does not exist."),
            @ApiResponse(code = 404, message = "User does not exist.")})
    @Path("/mark_as_read")
    public JsonResponse markAsRead(
            @ApiParam(name = "project", value = "Project id", required = true) String project,
            @ApiParam(name = "user", value = "User id", required = true) String user,
            @ApiParam(name = "message_ids", value = "The list of of message ids that will be marked as read", required = true) int[] message_ids) {
        storage.markMessagesAsRead(project, user, message_ids);
        return JsonResponse.success();
    }

    @Path("/get_online_users")
    @POST
    @JsonRequest
    @ApiOperation(value = "Get connected users",
            authorizations = @Authorization(value = "read_key")
    )
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Project does not exist.")})
    public CompletableFuture<Collection<Map<String, Object>>> getConnectedUsers(@ApiParam(name = "project", value = "Project id", required = true) String project) {
        Collection<Object> connectedUsers = webSocketService.getConnectedUsers(project);
        return CompletableFuture.completedFuture(connectedUsers.stream()
                .map(id -> ImmutableMap.of(config.getIdentifierColumn(), id))
                .collect(Collectors.toList()));
    }



}
