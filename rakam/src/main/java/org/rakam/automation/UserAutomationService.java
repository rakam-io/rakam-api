package org.rakam.automation;

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Expression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.inject.name.Named;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.util.JsonHelper;
import org.rakam.util.RakamException;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

public class UserAutomationService {

    private final DBI dbi;
    private final LoadingCache<String, List<AutomationRule>> rules;

    public UserAutomationService(@Named("report.metadata.store.jdbc") JDBCPoolDataSource dataSource) {
        dbi = new DBI(dataSource);

        rules = CacheBuilder.newBuilder().refreshAfterWrite(1, TimeUnit.MINUTES).build(new CacheLoader<String, List<AutomationRule>>() {
            @Override
            public List<AutomationRule> load(String project) throws Exception {
                try(Handle handle = dbi.open()) {
                    return handle.createQuery("SELECT user_filter, event_filter, actions FROM automation_rules")
                            .bind(":project", project).map((i, resultSet, statementContext) ->
                                new AutomationRule(project,
//                                        resultSet.getString(1),
                                        JsonHelper.read(resultSet.getString(2),EventFilter.class),
                                        JsonHelper.read(resultSet.getString(2), List.class))).list();
                }
            }
        });

        setup();
    }

    private void setup() {
        dbi.inTransaction((handle, transactionStatus) -> {
            handle.createStatement("CREATE TABLE IF NOT EXISTS automation_rules (" +
                    "  id SERIAL," +
                    "  project TEXT NOT NULL," +
                    "  user_filter TEXT NOT NULL," +
                    "  event_filter TEXT NOT NULL," +
                    "  actions TEXT NOT NULL," +
                    "  PRIMARY KEY (id)" +
                    "  )")
                    .execute();
            return null;
        });
    }

    public static class AutomationRule {
        private static final SqlParser SQL_PARSER = new SqlParser();

        public final String project;
//        public final Expression userFilter;
        public final EventFilter eventFilter;
        public final List<Action> actions;

        public AutomationRule(@ApiParam("project") String project,
//                              @ApiParam("user_filter") String userFilter,
                              @ApiParam("event_filter") EventFilter eventFilter,
                              @ApiParam("actions") List<Action> actions) {
            this.project = project;
//            this.userFilter = getUserFilter(userFilter);
            this.eventFilter = eventFilter;
            this.actions = actions;
        }

        @JsonIgnore
        private static synchronized Expression getUserFilter(String userFilter) {
            try {
                return userFilter != null ? SQL_PARSER.createExpression(userFilter) : null;
            } catch (Exception e) {
                throw new RakamException(format("filter expression '%s' couldn't parsed", userFilter), HttpResponseStatus.UNAUTHORIZED);
            }
        }
    }


    public void add(AutomationRule rule) {
        try(Handle handle = dbi.open()) {
//            handle.createStatement("INSERT INTO automation_rules (project, user_filter, event_filter, actions) VALUES (:project, :user_filter, :event_filter, :actions)")
            handle.createStatement("INSERT INTO automation_rules (project, event_filter, actions) VALUES (:project, :event_filter, :actions)")
                    .bind(":project", rule)
//                    .bind(":user_filter", rule.userFilter.toString())
                    .bind(":event_filter", JsonHelper.encode(rule.eventFilter))
                    .bind(":actions", JsonHelper.encode(rule.actions)).execute();
        }
    }


    public List<AutomationRule> get(String project) {
        try {
            return rules.get(project);
        } catch (ExecutionException e) {
            throw Throwables.propagate(e);
        }
    }

    static class EventFilter {
        private static final SqlParser SQL_PARSER = new SqlParser();

        public final String collection;
        public final String filterExpression;
        public final Threshold threshold;

        @JsonCreator
        public EventFilter(@JsonProperty("collection") String collection,
                           @JsonProperty("filter") String filterExpression,
                           @JsonProperty("threshold") Threshold threshold) {
            this.collection = collection;
            this.filterExpression = filterExpression;
            this.threshold = threshold;
        }

        @JsonIgnore
        public synchronized Expression getExpression() {
            try {
                return filterExpression  != null ? SQL_PARSER.createExpression(filterExpression) : null;
            } catch (Exception e) {
                throw new RakamException(format("filter expression '%s' couldn't parsed", filterExpression), HttpResponseStatus.UNAUTHORIZED);
            }
        }
    }

    public static class Threshold {
        public final ThresholdAggregation aggregation;
        public final String fieldName;
        public final long value;

        public Threshold(ThresholdAggregation aggregation, String fieldName, long value) {
            this.aggregation = aggregation;
            this.fieldName = fieldName;
            this.value = value;
        }
    }

    public enum ThresholdAggregation {
        count(false), sum(true);

        private final boolean fieldRequired;

        ThresholdAggregation(boolean fieldRequired) {
            this.fieldRequired = fieldRequired;
        }

        public boolean isFieldRequired() {
            return fieldRequired;
        }
    }

    public static class Action {
        public final ActionType type;
        public final String value;

        public Action(ActionType type, String value) {
            this.type = type;
            this.value = value;
        }
    }

    public enum ActionType {
        email, client
    }
}
