package org.rakam.plugin.user.mailbox;

/**
 * Created by buremba <Burak Emre Kabakcı> on 17/03/15 02:09.
 */
public class Message {
    public final String id;
    public final String content;
    public final boolean seen;

    public Message(String id, String content, boolean seen) {
        this.id = id;
        this.content = content;
        this.seen = seen;
    }
}
