package org.rakam.automation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.inject.name.Named;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.collection.Event;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.util.JsonHelper;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

public class UserAutomationService {

    private final DBI dbi;
    private final LoadingCache<String, List<AutomationRule>> rules;

    public UserAutomationService(@Named("report.metadata.store.jdbc") JDBCPoolDataSource dataSource) {
        dbi = new DBI(dataSource);

        rules = CacheBuilder.newBuilder().refreshAfterWrite(1, TimeUnit.MINUTES).build(new CacheLoader<String, List<AutomationRule>>() {
            @Override
            public List<AutomationRule> load(String project) throws Exception {
                try(Handle handle = dbi.open()) {
                    return handle.createQuery("SELECT id, user_filter, event_filter, actions FROM automation_rules WHERE project = :project")
                            .bind(":project", project).map((i, resultSet, statementContext) ->
                                    new AutomationRule(resultSet.getInt(1), project,
                                            JsonHelper.read(resultSet.getString(2), List.class),
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
                    "  event_filters TEXT NOT NULL," +
                    "  actions TEXT NOT NULL," +
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
        public final List<ScenarioStep> scenarioSteps;
        public final List<Action> actions;

        public AutomationRule(int id, String project, List<ScenarioStep> scenarioSteps, List<Action> actions) {
            this.id = id;
            this.project = project;
            this.scenarioSteps = scenarioSteps;
            this.actions = actions;
        }

        @JsonCreator
        public AutomationRule(@ApiParam(name="project") String project,
                              @ApiParam(name="scenarioSteps") List<ScenarioStep> scenarioSteps,
                              @ApiParam(name="actions") List<Action> actions) {
            this.id = -1;
            this.project = project;
            this.scenarioSteps = scenarioSteps;
            this.actions = actions;
        }
    }


    public void add(AutomationRule rule) {
        try(Handle handle = dbi.open()) {
            handle.createStatement("INSERT INTO automation_rules (project, event_filter, actions) VALUES (:project, :event_filter, :actions)")
                    .bind(":project", rule)
                    .bind(":event_filter", JsonHelper.encode(rule.scenarioSteps))
                    .bind(":actions", JsonHelper.encode(rule.actions)).execute();
        }
    }


    public List<AutomationRule> list(String project) {
        return rules.getUnchecked(project);
    }

    static class ScenarioStep {
        public final String collection;
        public final Threshold threshold;
        public final Predicate<Event> filterPredicate;

        @JsonCreator
        public ScenarioStep(@JsonProperty("collection") String collection,
                            @JsonProperty("filter") String filterExpression,
                            @JsonProperty("threshold") Threshold threshold) {
            this.collection = collection;
            this.threshold = threshold;

            if(filterExpression == null) {
                filterPredicate = (event) -> true;
            } else {
                filterPredicate = ExpressionCompiler.compile(filterExpression);
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
