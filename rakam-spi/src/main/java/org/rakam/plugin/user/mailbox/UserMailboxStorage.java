package org.rakam.plugin.user.mailbox;

import java.time.Instant;
import java.util.List;
import java.util.function.Consumer;


public interface UserMailboxStorage {
    public Message send(String project, Object fromUser, Object toUser, Integer parentId, String message, Instant date);
    public void createProject(String projectId);
    public MessageListener listen(String projectId, String user, Consumer<Data> messageConsumer);
    public MessageListener listenAllUsers(String projectId, Consumer<Data> messageConsumer);
    List<Message> getConversation(String project, Object userId, Integer parentId, int limit, long offset);
    public void markMessagesAsRead(String project, Object userId, int[] messageIds);

    public static interface MessageListener {
        public void shutdown();
    }


    public static enum Operation {
        msg, typing
    }

    public static class Data {
        public final Operation op;
        public final String payload;

        public Data(Operation op, String payload) {
            this.op = op;
            this.payload = payload;
        }

        public String serialize() {
            return op.name()+"\n"+payload;
        }
    }
}
