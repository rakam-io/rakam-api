package org.rakam.plugin;

import com.facebook.presto.sql.tree.Expression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.rakam.plugin.user.User;
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

    public static class Sorting {
        public final String column;
        public final Ordering order;

        @JsonCreator
        public Sorting(@JsonProperty("column") String column,
                       @JsonProperty("order") Ordering order) {
            this.column = column;
            this.order = order;
        }
    }

    public static enum Ordering {
        asc, desc
    }
}
