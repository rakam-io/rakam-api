package org.rakam.plugin.user.storage;

import com.facebook.presto.sql.tree.Expression;
import org.rakam.report.QueryResult;

import java.util.List;
import java.util.Map;

/**
 * Created by buremba <Burak Emre Kabakcı> on 15/03/15 21:32.
 */
public interface UserStorage {
    public void create(String project, Map<String, Object> properties);
    public QueryResult filter(String project, Expression filterExpression, int limit, int offset);
    public List<Column> getMetadata(String project);
}
