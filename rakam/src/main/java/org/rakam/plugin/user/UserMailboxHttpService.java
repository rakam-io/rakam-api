package org.rakam.plugin.user;

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.collection.SchemaField;
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
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.util.JsonResponse;
import org.rakam.util.RakamException;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.firstNonNull;
import static org.rakam.util.JsonHelper.encode;

/**
 * Created by buremba <Burak Emre Kabakcı> on 17/03/15 00:12.
 */
@Path("/user/mailbox")
@Api(value = "/user/mailbox", description = "UserMailbox", tags = {"user", "user-mailbox"})
public class UserMailboxHttpService extends HttpService {
    private final UserMailboxStorage storage;
    private final MailBoxWebSocketService webSocketService;
    private final UserStorage userStorage;
    private final UserPluginConfig config;
    private final SqlParser sqlParser;

    @Inject
    public UserMailboxHttpService(UserStorage userStorage, UserPluginConfig config, com.google.common.base.Optional<UserMailboxStorage> storage, MailBoxWebSocketService webSocketService) {
        this.userStorage = userStorage;
        this.storage = storage.orNull();
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
            authorizations = @Authorization(value = "api_key", type = "api_key")
    )
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Project does not exist."),
            @ApiResponse(code = 404, message = "User does not exist.")})
    public List<Message> get(@ApiParam(name = "project", value = "Project id", required = true) String project,
                             @ApiParam(name = "user", value = "User id", required = true) String user,
                             @ApiParam(name = "parent", value = "Parent message id", required = false) Integer parent,
                             @ApiParam(name = "limit", value = "Message query result limit", allowableValues = "range[1,100]", required = false) Integer limit,
                             @ApiParam(name = "offset", value = "Message query result offset", required = false) Long offset) {
        if(storage == null) {
            throw new RakamException("not implemented", 501);
        }
        return storage.getConversation(project, user, parent, firstNonNull(limit, 100), firstNonNull(offset, 0L));
    }

    @Path("/listen")
    @Consumes("text/event-stream")
    @GET
    @ApiImplicitParams({
            @ApiImplicitParam(name = "project", required = true, dataType = "string", paramType = "query"),
    })
    @ApiOperation(value = "Listen all mailboxes in a project",
            authorizations = @Authorization(value = "api_key", type = "api_key")
    )
    public void listen(RakamHttpRequest request) {
        if(storage == null) {
            request.response("not implemented", HttpResponseStatus.NOT_IMPLEMENTED).end();
            return;
        }
        RakamHttpRequest.StreamResponse response = request.streamResponse();

        List<String> project = request.params().get("project");
        if(project == null || project.isEmpty()) {
            response.send("result", encode(HttpServer.errorMessage("project query parameter is required", 400))).end();
            return;
        }

        UserMailboxStorage.MessageListener update = storage.listenAllUsers(project.get(0),
                data -> response.send("update", data.serialize()));

        response.listenClose(() -> update.shutdown());
    }

    /**
     * @api {post} /user/mailbox/mark_as_read Mark user mails as read
     * @apiVersion 0.1.0
     * @apiName MarkUserMailAsRead
     * @apiGroup userMailbox
     * @apiDescription Marks the specified mails as read.
     * @apiError Project does not exist.
     * @apiError User does not exist.
     * @apiError Message does not exist.
     * @apiSuccessExample {json} Success-Response:
     * HTTP/1.1 200 OK
     * {"success": true}
     * @apiParam {String} project   Project tracker code.
     * @apiParam {String} user    User ID
     * @apiParam {Number[]} message_ids  The list of of message ids that will be marked as read.
     * @apiExample {curl} Example usage:
     * curl 'http://localhost:9999/user/mailbox/mark_as_read' -H 'Content-Type: application/json;charset=UTF-8' --data-binary '{ "project": "projectId", "user": 12, "message_ids": [1] }'
     */
    @JsonRequest
    @ApiOperation(value = "Mark user mail as read",
            notes = "Marks the specified mails as read.",
            authorizations = @Authorization(value = "api_key", type = "api_key")
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
        if(storage == null) {
            throw new RakamException("not implemented", 501);
        }
        storage.markMessagesAsRead(project, user, message_ids);
        return JsonResponse.success();
    }

    /**
     * curl 'http://localhost:9999/user/mailbox/send' -H 'Content-Type: application/json;charset=UTF-8' --data-binary '{ "project": "projectId", "user": 1, "message": "Hello there!" }'
     */
    @Path("/send")
    @POST
    @JsonRequest
    @ApiOperation(value = "Send mail to user",
            notes = "Sends a mail to users mailbox",
            authorizations = @Authorization(value = "api_key", type = "api_key")
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
        if(storage == null) {
            throw new RakamException("not implemented", 501);
        }
        try {
            return storage.send(project, fromUser, toUser==null ? 0 : toUser, parent, message, Instant.ofEpochMilli(datetime));
        } catch (Exception e) {
            throw new RakamException("Error while sending message: "+e.getMessage(), 400);
        }
    }

    @Path("/getOnlineUsers")
    @POST
    @JsonRequest
    @ApiOperation(value = "Get connected users",
            authorizations = @Authorization(value = "api_key", type = "api_key")
    )
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Project does not exist.")})
    public CompletableFuture<Collection<Map<String, Object>>> getConnectedUsers(@ApiParam(name = "project", value = "Project id", required = true) String project,
                                                    @ApiParam(name = "properties", value = "User properties", required = false) List<String> properties) {

        if(storage == null) {
            throw new RakamException("not implemented", 501);
        }
        Collection<Object> connectedUsers = webSocketService.getConnectedUsers(project);
        if(properties == null) {
            return CompletableFuture.completedFuture(connectedUsers.stream()
                    .map(id -> ImmutableMap.of(config.getIdentifierColumn(), id))
                    .collect(Collectors.toList()));
        }

        if(connectedUsers.isEmpty()) {
            return CompletableFuture.completedFuture(ImmutableList.of());
        }

        String expressionStr = config.getIdentifierColumn() + " in (" + connectedUsers.stream()
                .map(id -> (id instanceof Number) ? id.toString() : "'" + id + "'")
                .collect(Collectors.joining(", ")) + ")";

        Expression expression;
        try {
            synchronized (sqlParser) {
                expression = sqlParser.createExpression(expressionStr);
            }
        } catch (Exception e) {
            throw new IllegalStateException();
        }

        return userStorage.filter(project, expression, null, null, 1000, 0).thenApply(queryResult -> {
            List<? extends SchemaField> metadata = queryResult.getMetadata();
            return queryResult.getResult().stream().map(row -> {
                Map<String, Object> map = new HashMap();
                for (int i = 0; i < metadata.size(); i++) {
                    map.put(metadata.get(i).getName(), row.get(i));
                }
                return map;
            }).collect(Collectors.toList());
        });
    }
}
