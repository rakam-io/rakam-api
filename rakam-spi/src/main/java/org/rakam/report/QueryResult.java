package org.rakam.report;

import org.rakam.collection.SchemaField;

import java.util.List;

/**
 * Created by buremba <Burak Emre Kabakcı> on 02/04/15 05:07.
 */
public interface QueryResult {
    public QueryError getError();
    boolean isFailed();
    List<List<Object>> getResult();
    public List<? extends SchemaField> getMetadata();
}
