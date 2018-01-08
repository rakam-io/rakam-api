package org.rakam.util;

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Statement;

import static com.google.common.base.Preconditions.checkNotNull;

public class SqlUtil {
    private final static SqlParser sqlParser = new SqlParser();

    public synchronized static Statement parseSql(String query) {
        return sqlParser.createStatement(checkNotNull(query, "query is required"));
    }

    public synchronized static Expression parseExpression(String query) {
        return sqlParser.createExpression(checkNotNull(query, "query is required"));
    }
}
