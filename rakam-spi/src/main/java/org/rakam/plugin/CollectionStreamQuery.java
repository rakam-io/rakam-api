package org.rakam.plugin;


import com.facebook.presto.sql.tree.Expression;

/**
 * Created by buremba <Burak Emre Kabakcı> on 02/04/15 08:23.
 */
public class CollectionStreamQuery {
    public final String collection;
    public final Expression filter;

    public CollectionStreamQuery(String collection, Expression filter) {
        this.collection = collection;
        this.filter = filter;
    }
}
