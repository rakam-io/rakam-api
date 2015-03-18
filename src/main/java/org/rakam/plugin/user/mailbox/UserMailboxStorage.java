package org.rakam.plugin.user.mailbox;

import org.rakam.plugin.user.mailbox.Envelope;
import org.rakam.plugin.user.mailbox.Message;
import org.rakam.plugin.user.mailbox.Ordering;

/**
 * Created by buremba <Burak Emre Kabakcı> on 16/03/15 21:33.
 */
public interface UserMailboxStorage {
    public void send(String userId, String message);
    public Envelope<Message> getMessages(String userId, Ordering order, int limit);
    public void markMessagesAsRead(String userId, int[] messageIds);
}
