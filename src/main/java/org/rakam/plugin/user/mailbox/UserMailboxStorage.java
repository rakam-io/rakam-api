package org.rakam.plugin.user.mailbox;

import java.util.List;

/**
 * Created by buremba <Burak Emre Kabakcı> on 16/03/15 21:33.
 */
public interface UserMailboxStorage {
    public void send(String userId, String message);
    public List<Message> getMessages(String userId, Ordering order, int limit);
    public void markMessagesAsRead(String userId, int[] messageIds);
}
