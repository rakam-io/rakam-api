package org.rakam.plugin.user.mailbox;

import java.time.Instant;
import java.util.List;
import java.util.function.Consumer;


public interface UserMailboxStorage {
    Message send(String project, Object fromUser, Object toUser, Integer parentId, String message, Instant date);

    void createProjectIfNotExists(String projectId, boolean userKeyIsNumeric);

    MessageListener listen(String projectId, String user, Consumer<Data> messageConsumer);

    MessageListener listenAllUsers(String projectId, Consumer<Data> messageConsumer);

    List<Message> getConversation(String project, String userId, Integer parentId, int limit, long offset);

    void markMessagesAsRead(String project, String userId, int[] messageIds);

    enum Operation {
        msg, typing
    }


    interface MessageListener {
        void shutdown();
    }

    class Data {
        public final Operation op;
        public final String payload;

        public Data(Operation op, String payload) {
            this.op = op;
            this.payload = payload;
        }

        public String serialize() {
            return op.name() + "\n" + payload;
        }
    }
}
