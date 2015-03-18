package org.rakam.plugin.user.mailbox;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.inject.Inject;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.annotations.JsonRequest;

import javax.ws.rs.Path;

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

    @JsonRequest
    @Path("/get")
    public Envelope<Message> get(GetMessagesQuery query) {
        return storage.getMessages(query.userId, Ordering.DESC, query.limit);
    }

    @JsonRequest
    @Path("/mark_as_read")
    public JsonNode markAsRead(MarkAsReadQuery query) {
        storage.markMessagesAsRead(query.userId, query.messageIds);
        return TextNode.valueOf("1");
    }

    @Path("/send")
    public JsonNode send(SendQuery query) {
        storage.send(query.userId, query.message);
        return TextNode.valueOf("1");
    }

    public static class GetMessagesQuery {
        public final String userId;
        public final int limit;

        public GetMessagesQuery(String userId, int limit) {
            this.userId = userId;
            this.limit = limit;
        }
    }

    public static class MarkAsReadQuery {
        public final String userId;
        public final int[] messageIds;

        public MarkAsReadQuery(String userId, int[] messageIds) {
            this.userId = userId;
            this.messageIds = messageIds;
        }
    }

    public static class SendQuery {
        public final String userId;
        public final String message;

        public SendQuery(String userId, String message) {
            this.userId = userId;
            this.message = message;
        }
    }
}
