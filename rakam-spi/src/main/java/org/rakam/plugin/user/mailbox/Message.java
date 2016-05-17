package org.rakam.plugin.user.mailbox;


public class Message {
    public final int id;
    public final String content;
    public final Object fromUser;
    public final Object toUser;
    public final Integer parentId;
    public final boolean seen;
    public final long time;

    public Message(int id, Object fromUser, Object toUser, String content, Integer parentId, boolean seen, long time) {
        this.id = id;
        this.fromUser = fromUser;
        this.toUser = toUser;
        this.content = content;
        this.parentId = parentId;
        this.seen = seen;
        this.time = time;
    }
}
