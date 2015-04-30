package org.rakam.plugin.user;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.inject.Inject;
import org.rakam.plugin.user.mailbox.Message;
import org.rakam.plugin.user.mailbox.UserMailboxStorage;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.annotations.Api;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.server.http.annotations.ApiResponse;
import org.rakam.server.http.annotations.ApiResponses;
import org.rakam.server.http.annotations.Authorization;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.util.json.JsonResponse;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import java.time.Instant;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

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
        return new JsonResponse() {
            public final boolean success = true;
        };
    }

    /**
     * @api {post} /user/mailbox/send Send mail to user
     * @apiVersion 0.1.0
     * @apiName SendMailToUser
     * @apiGroup userMailbox
     * @apiDescription Sends a mail to users mailbox
     * @apiError Project does not exist.
     * @apiError User does not exist.
     * @apiSuccessExample {json} Success-Response:
     * HTTP/1.1 200 OK
     * {"success": true}
     * @apiParam {String} project   Project tracker code.
     * @apiParam user   User ID
     * @apiParam {String} message    The content of the message.
     * @apiExample {curl} Example usage:
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
    public JsonResponse send(@ApiParam(name = "project", value = "Project id", required = true) String project,
                             @ApiParam(name = "from_user", required = true) String fromUser,
                             @ApiParam(name = "to_user", required = true) String toUser,
                             @ApiParam(name = "parent", value = "Parent message id", required = false) Integer parent,
                             @ApiParam(name = "message", value = "The content of the message", required = false) String message,
                             @ApiParam(name = "timestamp", value = "The zoned datetime of the message", required = true) Instant datetime) {
        storage.send(project, fromUser, toUser, parent, message, datetime);
        return new JsonResponse() {
            public final boolean success = true;
        };
    }

    public static class GetMessagesQuery {
        @ApiParam(name = "project", value = "Project id", required = true)
        public final String project;
        @ApiParam(name = "user", value = "User id", required = true)
        public final String user;
        @ApiParam(name = "limit", value = "Message query result limit", allowableValues = "range[1,100]", required = false)
        public final int limit;
        @ApiParam(name = "offset", value = "Message query result offset", required = false)
        public final int offset;

        @JsonCreator
        public GetMessagesQuery(@JsonProperty("project") String project,
                                @JsonProperty("user") String user,
                                @JsonProperty("limit") int limit,
                                @JsonProperty("offset") int offset) {
            this.project = checkNotNull(project, "project is required");
            this.user = checkNotNull(user, "user is required");
            this.limit = limit;
            this.offset = offset;
        }
    }
}
