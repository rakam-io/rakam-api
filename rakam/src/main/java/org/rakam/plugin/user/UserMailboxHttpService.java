package org.rakam.plugin.user;

import com.google.inject.Inject;
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

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import java.time.Instant;
import java.util.List;

import static org.rakam.util.JsonHelper.encode;

/**
 * Created by buremba <Burak Emre Kabakcı> on 17/03/15 00:12.
 */
@Path("/user/mailbox")
@Api(value = "/user/mailbox", description = "UserMailbox", tags = {"user", "user-mailbox"})
public class UserMailboxHttpService extends HttpService {
    private final UserMailboxStorage storage;

    @Inject
    public UserMailboxHttpService(UserMailboxStorage storage) {
        this.storage = storage;
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
                             @ApiParam(name = "limit", value = "Message query result limit", allowableValues = "range[1,100]", required = false) int limit,
                             @ApiParam(name = "offset", value = "Message query result offset", required = false) int offset) {
        return storage.getConversation(project, user, parent, limit, offset);
    }

    @Path("/listen")
    @Consumes("text/event-stream")
    @GET
    @ApiImplicitParams({
            @ApiImplicitParam(name = "project", value = "User's name", required = true, dataType = "string", paramType = "query"),
    })
    @ApiOperation(value = "Listen all mailboxes in a project",
            authorizations = @Authorization(value = "api_key", type = "api_key")
    )
    public void listen(RakamHttpRequest request) {
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
                        @ApiParam(name = "to_user", required = false) String toUser,
                        @ApiParam(name = "parent", value = "Parent message id", required = false) Integer parent,
                        @ApiParam(name = "message", value = "The content of the message", required = false) String message,
                        @ApiParam(name = "timestamp", value = "The zoned datetime of the message", required = true) Instant datetime) {
        try {
            return storage.send(project, fromUser, toUser==null ? 0 : toUser, parent, message, datetime);
        } catch (Exception e) {
            throw new RakamException("Error while sending message: "+e.getMessage(), 400);
        }
    }
}
