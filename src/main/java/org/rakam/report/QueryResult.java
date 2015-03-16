package org.rakam.report;

import org.rakam.report.metadata.Column;
import java.util.List;

/**
* Created by buremba <Burak Emre Kabakcı> on 15/03/15 21:54.
*/
public class QueryResult {
    private final List<Column> columns;
    private final List<List<Object>> result;
    private final QueryError error;

    public QueryResult(List<Column> columns, List<List<Object>> result, QueryError error) {
        this.columns = columns;
        this.result = result;
        this.error = error;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public List<List<Object>> getResult() {
        return result;
    }

    public boolean isFailed() {
        return error != null;
    }
    public QueryError getError() {
        return error;
    }
}
