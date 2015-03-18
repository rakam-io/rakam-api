package org.rakam.plugin.user.mailbox;

/**
 * Created by buremba <Burak Emre Kabakcı> on 17/03/15 03:00.
 */
public class Envelope<T> {
    public final T content;
    public final boolean seen;

    public Envelope(T content, boolean seen) {
        this.content = content;
        this.seen = seen;
    }
}
