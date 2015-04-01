package org.rakam.plugin.user.mailbox;

import java.util.List;

/**
 * Created by buremba <Burak Emre Kabakcı> on 16/03/15 21:33.
 */
public interface UserMailboxStorage {
    public Message send(String project, Object userId, String message);
    public void createProject(String projectId);
    public List<Message> getMessages(String project, Object userId, int limit, int offset);
    public void markMessagesAsRead(String project, Object userId, int[] messageIds);
}
