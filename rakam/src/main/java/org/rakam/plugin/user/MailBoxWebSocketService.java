package org.rakam.plugin.user;

import com.google.inject.Inject;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.AttributeKey;
import org.rakam.plugin.user.mailbox.UserMailboxStorage;
import org.rakam.server.http.WebSocketService;
import org.rakam.server.http.annotations.Api;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.server.http.annotations.Authorization;
import org.rakam.util.JsonHelper;

import javax.ws.rs.Path;
import java.io.IOException;
import java.time.Instant;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Created by buremba <Burak Emre Kabakcı> on 31/03/15 04:21.
 */
@Path("/user/mailbox/subscribe")
@Api(value = "/user/mailbox/subscribe", description = "Websocket service for subscribing user mails in real-time",
        tags = "user", consumes = "ws", produces = "ws", protocols = "ws")
public class MailBoxWebSocketService extends WebSocketService {
    public static final AttributeKey<String> USER_ID = AttributeKey.valueOf("user_id");
    public static final AttributeKey<String> PROJECT_ID = AttributeKey.valueOf("project_id");
    private final UserMailboxStorage storage;

    @Inject
    public MailBoxWebSocketService(UserMailboxStorage storage) {
        this.storage = storage;
    }

    @Override
    public void onOpen(WebSocketRequest request) {
        List<String> user = request.params().get("user");
        List<String> project = request.params().get("project");
        if(user != null && user.size()>0 && project !=null && project.size()>0) {
            ChannelHandlerContext context = request.context();
            context.attr(USER_ID).set(user.get(0));
            context.attr(PROJECT_ID).set(project.get(0));
        } else {
            request.context().close();
        }
    }

    @Override
    @ApiOperation(value = "Realtime mailbox notification service",
            notes = "Websocket service for sending and receiving mail notification",
            response = WSMessage.class,
            request = String.class,
            responseContainer = "List",
            authorizations = @Authorization(value = "api_key", type = "api_key")
    )
    public void onMessage(ChannelHandlerContext ctx, String data) {
        Integer jsonStart = data.indexOf("\n");
        Operation op = Operation.valueOf(data.substring(0, jsonStart));
        String jsonStr = data.substring(jsonStart);
        switch (op) {
            case msg:
                try {
                    UserMessage message = JsonHelper.readSafe(jsonStr, UserMessage.class);
                    storage.send(ctx.attr(PROJECT_ID).get(), message.from_user, message.to_user,
                            message.parent, message.message, Instant.now());
                } catch (IOException e) {
                    ctx.close();
                }
                break;
            case typing:
                break;

        }
    }

    @Override
    public void onClose(ChannelHandlerContext ctx) {
        System.out.println(1);
    }

    public static class UserMessage {
        public final String project;
        public final Object from_user;
        public final Object to_user;
        public final Integer parent;
        public final String message;

        public UserMessage(String project, Object from_user, Object to_user, Integer parent, String message) {
            this.project = project;
            this.from_user = from_user;
            this.to_user = to_user;
            this.parent = parent;
            this.message = message;
        }
    }
    public static class WSMessage {
        public final int time;
        public final Operation op;
        public final String value;

        public WSMessage(int time, Operation op, String value) {
            this.time = checkNotNull(time, "time is required");
            this.op = checkNotNull(op, "op is required");
            this.value = value;
        }
    }

    public static enum Operation {
        msg, typing
    }
}
