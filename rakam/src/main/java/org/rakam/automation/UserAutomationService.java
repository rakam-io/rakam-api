package org.rakam.automation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.inject.name.Named;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.collection.Event;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.util.JsonHelper;
import org.rakam.util.RakamException;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;

import javax.inject.Inject;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

public class UserAutomationService {

    private final DBI dbi;
    private final LoadingCache<String, List<AutomationRule>> rules;

    @Inject
    public UserAutomationService(@Named("report.metadata.store.jdbc") JDBCPoolDataSource dataSource) {
        dbi = new DBI(dataSource);

        rules = CacheBuilder.newBuilder().refreshAfterWrite(1, TimeUnit.MINUTES).build(new CacheLoader<String, List<AutomationRule>>() {
            @Override
            public List<AutomationRule> load(String project) throws Exception {
                try(Handle handle = dbi.open()) {
                    return handle.createQuery("SELECT id, event_filters, actions, custom_data FROM automation_rules WHERE project = :project")
                            .bind("project", project)
                            .map((i, resultSet, statementContext) ->
                                    new AutomationRule(resultSet.getInt(1), project,
                                            Arrays.asList(JsonHelper.read(resultSet.getString(2), ScenarioStep[].class)),
                                            Arrays.asList(JsonHelper.read(resultSet.getString(3), Action[].class)),
                                            resultSet.getString(4)))
                            .list();
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
                    "  event_filters TEXT NOT NULL," +
                    "  actions TEXT NOT NULL," +
                    "  custom_data TEXT," +
                    "  PRIMARY KEY (id)" +
                    "  )")
                    .execute();
            return null;
        });
    }

    public void remove(String project, int id) {

    }

    public static class AutomationRule {
        public final int id;
        public final String project;
        public final List<ScenarioStep> scenarios;
        public final List<Action> actions;
        public final String customData;

        public AutomationRule(int id, String project, List<ScenarioStep> scenarios, List<Action> actions, String customData) {
            this.id = id;
            this.project = project;
            this.scenarios = scenarios;
            this.actions = actions;
            this.customData = customData;
        }

        @JsonCreator
        public AutomationRule(@ApiParam(name = "project") String project,
                              @ApiParam(name = "scenarios") List<ScenarioStep> scenarios,
                              @ApiParam(name = "actions") List<Action> actions,
                              @ApiParam(name = "customData") String customData) {
            this.customData = customData;
            this.id = -1;
            this.project = project;
            this.scenarios = scenarios;
            this.actions = actions;
        }
    }

    public void add(AutomationRule rule) {
        try(Handle handle = dbi.open()) {
            handle.createStatement("INSERT INTO automation_rules (project, event_filters, actions, custom_data) VALUES (:project, :event_filters, :actions, :custom_data)")
                    .bind("project", rule.project)
                    .bind("event_filters", JsonHelper.encode(rule.scenarios))
                    .bind("custom_data", rule.customData)
                    .bind("actions", JsonHelper.encode(rule.actions)).execute();
        }
    }


    public List<AutomationRule> list(String project) {
        return rules.getUnchecked(project);
    }

    static class ScenarioStep {
        private static final Threshold DEFAULT_THRESHOLD = new Threshold(ThresholdAggregation.count, null, 0);
        public final String collection;
        public final Threshold threshold;
        @JsonIgnore
        public final Predicate<Event> filterPredicate;
        @JsonProperty
        private final String filter;

        @JsonCreator
        public ScenarioStep(@JsonProperty("collection") String collection,
                            @JsonProperty("filter") String filterExpression,
                            @JsonProperty("threshold") Threshold threshold) {
            this.collection = collection;
            this.filter = filterExpression;
            this.threshold = threshold == null ? DEFAULT_THRESHOLD : threshold;

            if(filterExpression == null || filterExpression.isEmpty()) {
                filterPredicate = (event) -> true;
            } else {
                try {
                    filterPredicate = ExpressionCompiler.compile(filterExpression);
                } catch (UnsupportedOperationException e) {
                    throw new RakamException("Unable to compile filter expression", HttpResponseStatus.NOT_IMPLEMENTED);
                }
            }
        }


    }

    public static class Threshold {
        public final ThresholdAggregation aggregation;
        public final String fieldName;
        public final long value;

        @JsonCreator
        public Threshold(@JsonProperty("aggregation") ThresholdAggregation aggregation,
                         @JsonProperty("fieldName") String fieldName,
                         @JsonProperty("value") long value) {
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

        @JsonCreator
        public Action(@JsonProperty("type") ActionType type,
                      @JsonProperty("value") String value) {
            this.type = type;
            this.value = value;
        }
    }

    public enum ActionType {
        email, client
    }
}
