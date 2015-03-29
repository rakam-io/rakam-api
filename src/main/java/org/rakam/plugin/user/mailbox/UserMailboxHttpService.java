package org.rakam.plugin.user.mailbox;

import com.google.inject.Inject;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.util.json.JsonResponse;

import javax.ws.rs.Path;
import java.util.List;

/**
 * Created by buremba <Burak Emre Kabakcı> on 17/03/15 00:12.
 */
@Path("/user/mailbox")
public class UserMailboxHttpService implements HttpService {
    private final UserMailboxStorage storage;

    @Inject
    public UserMailboxHttpService(UserMailboxStorage storage) {
        this.storage = storage;
    }

    /**
     * @api {post} /user/mailbox/get Get user mailbox
     * @apiVersion 0.1.0
     * @apiName GetUserMailbox
     * @apiGroup userMailbox
     * @apiDescription Returns the last mails sent to the user
     *
     * @apiError Project does not exist.
     * @apiError User does not exist.
     *
     * @apiSuccessExample {json} Success-Response:
     *     HTTP/1.1 200 OK
     *     {"messages": [{"id": 1, "content": "Hello there!", "seen": false}]}
     *
     * @apiParam {String} project   Project tracker code
     * @apiParam {String} user    User ID
     * @apiParam {Object} limit    Maximum number of mails will be returned
     *
     * @apiSuccess (200) {Object[]} messages  List of messages
     * @apiSuccess (200) {Number} messages.id  List of messages
     * @apiSuccess (200) {String} messages.content  List of messages
     * @apiSuccess (200) {Boolean} messages.seen  List of messages
     *
     * @apiExample {curl} Example usage:
     *     curl 'http://localhost:9999/user/mailbox/get' -H 'Content-Type: application/json;charset=UTF-8' --data-binary '{ "project": "projectId", "user": 1} }'
     */
    @JsonRequest
    @Path("/get")
    public JsonResponse get(GetMessagesQuery query) {
        return new JsonResponse() {
            public final List<Message> messages = storage.getMessages(query.userId, Ordering.DESC, query.limit);
        };
    }

    /**
     * @api {post} /user/mailbox/mark_as_read Mark user mails as read
     * @apiVersion 0.1.0
     * @apiName MarkUserMailAsRead
     * @apiGroup userMailbox
     * @apiDescription Marks the specified mails as read.
     *
     * @apiError Project does not exist.
     * @apiError User does not exist.
     * @apiError Message does not exist.
     *
     * @apiSuccessExample {json} Success-Response:
     *     HTTP/1.1 200 OK
     *     {"success": true}
     *
     * @apiParam {String} project   Project tracker code.
     * @apiParam {String} user    User ID
     * @apiParam {Number[]} message_ids  Tge list of of message ids that will be marked as read.
     *
     * @apiExample {curl} Example usage:
     *     curl 'http://localhost:9999/user/mailbox/mark_as_read' -H 'Content-Type: application/json;charset=UTF-8' --data-binary '{ "project": "projectId", "user": 12, "message_ids": [1] }'
     */
    @JsonRequest
    @Path("/mark_as_read")
    public JsonResponse markAsRead(MarkAsReadQuery query) {
        storage.markMessagesAsRead(query.user, query.messageIds);
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
     *
     * @apiError Project does not exist.
     * @apiError User does not exist.
     *
     * @apiSuccessExample {json} Success-Response:
     *     HTTP/1.1 200 OK
     *     {"success": true}
     *
     * @apiParam {String} project   Project tracker code.
     * @apiParam user   User ID
     * @apiParam {String} message    The content of the message.
     *
     * @apiExample {curl} Example usage:
     *     curl 'http://localhost:9999/user/mailbox/send' -H 'Content-Type: application/json;charset=UTF-8' --data-binary '{ "project": "projectId", "user": 1, "message": "Hello there!" }'
     */
    @Path("/send")
    public JsonResponse send(SendQuery query) {
        storage.send(query.user, query.message);
        return new JsonResponse() {
            public final boolean success = true;
        };
    }

    public static class GetMessagesQuery {
        public final String project;
        public final String userId;
        public final int limit;

        public GetMessagesQuery(String project, String userId, int limit) {
            this.project = project;
            this.userId = userId;
            this.limit = limit;
        }
    }

    public static class MarkAsReadQuery {
        public final String project;
        public final String user;
        public final int[] messageIds;

        public MarkAsReadQuery(String project, String user, int[] messageIds) {
            this.project = project;
            this.user = user;
            this.messageIds = messageIds;
        }
    }

    public static class SendQuery {
        public final String project;
        public final String user;
        public final String message;

        public SendQuery(String project, String user, String message) {
            this.project = project;
            this.user = user;
            this.message = message;
        }
    }
}
