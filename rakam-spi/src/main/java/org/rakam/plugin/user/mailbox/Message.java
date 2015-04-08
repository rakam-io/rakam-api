package org.rakam.plugin.user.mailbox;

/**
 * Created by buremba <Burak Emre Kabakcı> on 17/03/15 02:09.
 */
public class Message {
    public final Object id;
    public final String content;
    public final Object parentId;
    public final boolean seen;

    public Message(Object id, String content, Object parentId, boolean seen) {
        this.id = id;
        this.content = content;
        this.parentId = parentId;
        this.seen = seen;
    }
}
