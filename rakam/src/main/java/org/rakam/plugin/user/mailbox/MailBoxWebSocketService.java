package org.rakam.plugin.user.mailbox;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Singleton;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.util.AttributeKey;
import org.rakam.plugin.user.mailbox.UserMailboxStorage.MessageListener;
import org.rakam.plugin.user.mailbox.UserMailboxStorage.Operation;
import org.rakam.server.http.WebSocketService;
import org.rakam.server.http.annotations.Api;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.server.http.annotations.Authorization;
import org.rakam.util.JsonHelper;

import javax.inject.Inject;
import javax.ws.rs.Path;
import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

@Path("/user/mailbox/subscribe")
@Api(value = "/user/mailbox/subscribe", description = "Websocket service for subscribing user mails in real-time",
        tags = "user", consumes = "ws", produces = "ws", protocols = "ws")
@Singleton
public class MailBoxWebSocketService extends WebSocketService {
    public static final AttributeKey<String> USER_ID = AttributeKey.valueOf("user_id");
    public static final AttributeKey<String> PROJECT_ID = AttributeKey.valueOf("project_id");
    public static final AttributeKey<MessageListener> LISTENER = AttributeKey.valueOf("listener");
    private final UserMailboxStorage storage;
    private final Map<String, Map<Object, List<Channel>>> connectedClients = new ConcurrentHashMap<>();

    @Inject
    public MailBoxWebSocketService(com.google.common.base.Optional<UserMailboxStorage> storage) {
        this.storage = storage.orNull();
    }

    @Override
    public void onOpen(WebSocketRequest request) {
        if (storage == null) {
            // TODO: inform user.
            request.context().close();
        }
        List<String> userParam = request.params().get("user");
        List<String> projectParam = request.params().get("project");

        String project;
        String user;
        if (userParam != null && !userParam.isEmpty() && projectParam != null && !projectParam.isEmpty()) {
            user = userParam.get(0);
            project = projectParam.get(0);
            ChannelHandlerContext context = request.context();
            context.attr(USER_ID).set(user);
            context.attr(PROJECT_ID).set(project);
            connectedClients
                    .computeIfAbsent(project, s -> Maps.newConcurrentMap())
                    .computeIfAbsent(user, s -> Lists.newArrayList()).add(context.channel());
        } else {
            request.context().close();
            return;
        }

        MessageListener listen = storage.listen(project, user, data -> {
            Map<Object, List<Channel>> users = connectedClients.get(project);
            if (users != null) {
                List<Channel> channels = users.get(user);
                if (channels != null) {
                    channels.forEach(channel -> channel.writeAndFlush(new TextWebSocketFrame(data.op + "\n" + data.payload)));
                }
            }
        });
        request.context().attr(LISTENER).set(listen);
    }

    @Override
    @ApiOperation(value = "Realtime mailbox notification service",
            notes = "Websocket service for sending and receiving mail noti  fication",
            response = WSMessage.class,
            request = String.class,
            responseContainer = "List",
            authorizations = {@Authorization(value = "write_key"), @Authorization(value = "read_key")}
    )

    public void onMessage(ChannelHandlerContext ctx, String data) {
        Integer jsonStart = data.indexOf("\n");
        Operation op = Operation.valueOf(data.substring(0, jsonStart));
        String jsonStr = data.substring(jsonStart);
        switch (op) {
            case msg:
                try {
                    UserMessage message = JsonHelper.readSafe(jsonStr, UserMessage.class);
                    storage.send(ctx.attr(PROJECT_ID).get(), ctx.attr(USER_ID).get(), message.toUser,
                            message.parent, message.content, Instant.now());
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
        connectedClients
                .computeIfAbsent(ctx.attr(PROJECT_ID).get(), s -> Maps.newConcurrentMap())
                .computeIfAbsent(ctx.attr(USER_ID).get(), s -> Lists.newArrayList()).remove(ctx.channel());
        ctx.attr(LISTENER).get().shutdown();
    }

    public Collection<Object> getConnectedUsers(String project) {
        Map<Object, List<Channel>> objectListMap = connectedClients.get(project);
        if (objectListMap == null)
            return ImmutableList.of();
        return objectListMap.entrySet().stream()
                .filter(e -> !e.getValue().isEmpty())
                .map(e -> e.getKey()).collect(Collectors.toList());
    }

    public static class UserMessage {
        public final Integer parent;
        public final String content;
        public final String toUser;

        @JsonCreator
        public UserMessage(@JsonProperty("parent") Integer parent,
                           @JsonProperty("message") String content,
                           @JsonProperty("to_user") String toUser) {
            this.parent = parent;
            this.content = content;
            this.toUser = toUser;
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
}
