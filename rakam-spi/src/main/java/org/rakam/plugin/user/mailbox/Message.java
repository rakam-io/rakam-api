package org.rakam.plugin.user.mailbox;

import java.time.Instant;

/**
 * Created by buremba <Burak Emre Kabakcı> on 17/03/15 02:09.
 */
public class Message {
    public final int id;
    public final String content;
    public final Object from_user;
    public final Integer parentId;
    public final boolean seen;
    public final Instant time;

    public Message(int id, Object from_user, String content, Integer parentId, boolean seen, Instant time) {
        this.id = id;
        this.from_user = from_user;
        this.content = content;
        this.parentId = parentId;
        this.seen = seen;
        this.time = time;
    }
}
