package org.rakam.plugin.user.mailbox;

import com.fasterxml.jackson.databind.JsonNode;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.AttributeKey;
import org.rakam.server.http.WebSocketService;
import org.rakam.util.JsonHelper;

import javax.ws.rs.Path;
import java.io.IOException;
import java.util.List;

/**
 * Created by buremba <Burak Emre Kabakcı> on 31/03/15 04:21.
 */
@Path("/user/mailbox/subscribe")
public class MailBoxWebSocketService extends WebSocketService {
    public static final AttributeKey<String> USER_ID = AttributeKey.valueOf("user_id");

    public MailBoxWebSocketService() {
    }

    @Override
    public void onOpen(WebSocketRequest request) {
        List<String> user = request.params().get("user");
        if(user != null && user.size()>0) {
            ChannelHandlerContext context = request.context();
            context.attr(USER_ID).set(user.get(0));
        } else {
            request.context().close();
        }
    }

    @Override
    public void onMessage(ChannelHandlerContext ctx, String message) {
        JsonNode json;
        try {
            json = JsonHelper.readSafe(message);
        } catch (IOException e) {
            return;
        }
        JsonNode op = json.get("op");
        if (op == null) {
            return;
        }
        switch (op.asText()) {
            case "login":

        }
        System.out.println(1);
    }

    @Override
    public void onClose(ChannelHandlerContext ctx) {
        System.out.println(1);
    }
}
