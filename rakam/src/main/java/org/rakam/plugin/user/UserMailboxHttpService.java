package org.rakam.plugin.user;

import com.facebook.presto.sql.parser.SqlParser;
import com.google.common.collect.ImmutableMap;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.plugin.UserPluginConfig;
import org.rakam.plugin.UserStorage;
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
import org.rakam.util.RakamException;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import java.time.Instant;
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
    private final UserStorage userStorage;
    private final UserPluginConfig config;
    private final SqlParser sqlParser;

    @Inject
    public UserMailboxHttpService(UserStorage userStorage, UserPluginConfig config, UserMailboxStorage storage, MailBoxWebSocketService webSocketService) {
        this.userStorage = userStorage;
        this.storage = storage;
        this.config = config;
        this.webSocketService = webSocketService;
        this.sqlParser = new SqlParser();
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
    public void listen(RakamHttpRequest request) {
        RakamHttpRequest.StreamResponse response = request.streamResponse();

        List<String> project = request.params().get("project");
        if(project == null || project.isEmpty()) {
            response.send("result", encode(HttpServer.errorMessage("project query parameter is required", HttpResponseStatus.BAD_REQUEST))).end();
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

    @Path("/send")
    @POST
    @JsonRequest
    @ApiOperation(value = "Send mail to user",
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
            return storage.send(project, fromUser, toUser==null ? 0 : toUser, parent, message, Instant.ofEpochMilli(datetime));
        } catch (Exception e) {
            throw new RakamException("Error while sending message: "+e.getMessage(), HttpResponseStatus.BAD_REQUEST);
        }
    }

    @Path("/getOnlineUsers")
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

        // implement filter by user properties
//        if(connectedUsers.isEmpty()) {
//            return CompletableFuture.completedFuture(ImmutableList.of());
//        }
//
//        String expressionStr = config.getIdentifierColumn() + " in (" + connectedUsers.stream()
//                .map(id -> (id instanceof Number) ? id.toString() : "'" + id + "'")
//                .collect(Collectors.joining(", ")) + ")";
//
//        Expression expression;
//        try {
//            synchronized (sqlParser) {
//                expression = sqlParser.createExpression(expressionStr);
//            }
//        } catch (Exception e) {
//            throw new IllegalStateException();
//        }
//
//        return userStorage.filter(project, expression, null, null, 1000, 0).thenApply(queryResult -> {
//            List<? extends SchemaField> metadata = queryResult.getMetadata();
//            return queryResult.getResult().stream().map(row -> {
//                Map<String, Object> map = new HashMap();
//                for (int i = 0; i < metadata.size(); i++) {
//                    map.put(metadata.get(i).getName(), row.get(i));
//                }
//                return map;
//            }).collect(Collectors.toList());
//        });
    }
}
