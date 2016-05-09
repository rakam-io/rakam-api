package org.rakam.plugin.user.mailbox;


public class Message {
    public final int id;
    public final String content;
    public final Object from_user;
    public final Object to_user;
    public final Integer parentId;
    public final boolean seen;
    public final long time;

    public Message(int id, Object from_user, Object to_user, String content, Integer parentId, boolean seen, long time) {
        this.id = id;
        this.from_user = from_user;
        this.to_user = to_user;
        this.content = content;
        this.parentId = parentId;
        this.seen = seen;
        this.time = time;
    }
}
