package org.rakam.plugin.user.storage;

import com.facebook.presto.sql.tree.Expression;
import org.rakam.plugin.user.User;
import org.rakam.plugin.user.UserHttpService.UserQuery.Sorting;
import org.rakam.report.QueryResult;

import java.util.List;
import java.util.Map;

/**
 * Created by buremba <Burak Emre Kabakcı> on 15/03/15 21:32.
 */
public interface UserStorage {
    public void create(String project, Map<String, Object> properties);
    public QueryResult filter(String project, Expression filterExpression, Sorting sortColumn, int limit, int offset);
    public List<Column> getMetadata(String project);
    public User getUser(String project, Object userId);
    void setUserProperty(String project, Object user, String property, Object value);
}
