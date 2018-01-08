package org.rakam.automation;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.inject.name.Named;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.util.JsonHelper;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;

import javax.inject.Inject;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class UserAutomationService {

    private final DBI dbi;
    private final LoadingCache<String, List<AutomationRule>> rules;
    private final Set<AutomationAction> automationActions;

    @Inject
    public UserAutomationService(@Named("report.metadata.store.jdbc") JDBCPoolDataSource dataSource, Set<AutomationAction> automationActions) {
        dbi = new DBI(dataSource);
        this.automationActions = automationActions;

        rules = CacheBuilder.newBuilder().refreshAfterWrite(1, TimeUnit.MINUTES).build(new CacheLoader<String, List<AutomationRule>>() {
            @Override
            public List<AutomationRule> load(String project) throws Exception {
                try (Handle handle = dbi.open()) {
                    return handle.createQuery("SELECT id, is_active, event_filters, actions, custom_data FROM automation_rules WHERE project = :project")
                            .bind("project", project)
                            .map((i, resultSet, statementContext) -> {
                                List<AutomationRule.SerializableAction> actions = Arrays.asList(JsonHelper.read(resultSet.getString(4), AutomationRule.SerializableAction[].class));

                                for (AutomationRule.SerializableAction action : actions) {
                                    AutomationAction<?> automationAction = automationActions.stream()
                                            .filter(a -> a.getClass().equals(action.type.getActionClass()))
                                            .findFirst().get();
                                    action.setAction(automationAction);
                                }
                                return new AutomationRule(resultSet.getInt(1),
                                        resultSet.getBoolean(2), Arrays.asList(JsonHelper.read(resultSet.getString(3), AutomationRule.ScenarioStep[].class)),
                                        actions,
                                        resultSet.getString(5));
                            })
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
                    "  is_active BOOLEAN NOT NULL," +
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
        try (Handle handle = dbi.open()) {
            handle.createStatement("DELETE FROM automation_rules WHERE project = :project AND id = :id")
                    .bind("project", project)
                    .bind("id", id).execute();
        }
        rules.refresh(project);
    }

    public void deactivate(String project, int id) {
        try (Handle handle = dbi.open()) {
            handle.createStatement("UPDATE automation_rules SET is_active = false WHERE project = :project AND id = :id")
                    .bind("project", project)
                    .bind("id", id).execute();
        }
        Optional<AutomationRule> any = rules.getUnchecked(project).stream().filter(r -> r.id == id).findAny();
        if (any.isPresent()) {
            any.get().setActive(false);
        } else {
            rules.refresh(project);
        }
    }

    public void activate(String project, int id) {
        try (Handle handle = dbi.open()) {
            handle.createStatement("UPDATE automation_rules SET is_active = true WHERE project = :project AND id = :id")
                    .bind("project", project)
                    .bind("id", id).execute();
        }
        Optional<AutomationRule> any = rules.getUnchecked(project).stream().filter(r -> r.id == id).findAny();
        if (any.isPresent()) {
            any.get().setActive(true);
        } else {
            rules.refresh(project);
        }
    }

    public void add(String project, AutomationRule rule) {
        for (AutomationRule.SerializableAction action : rule.actions) {
            AutomationAction<?> automationAction = automationActions.stream()
                    .filter(a -> a.getClass().equals(action.type.getActionClass()))
                    .findFirst().get();
            Type[] genericInterfaces = automationAction.getClass().getGenericInterfaces();
            Class optionsClass = null;
            for (Type genericInterface : genericInterfaces) {
                if (genericInterface instanceof ParameterizedType &&
                        ((ParameterizedType) genericInterface).getRawType().equals(AutomationAction.class)) {
                    optionsClass = (Class) ((ParameterizedType) genericInterface).getActualTypeArguments()[0];
                }
            }
            if (optionsClass == null) {
                throw new IllegalStateException();
            }

            JsonHelper.convert(action.value, optionsClass);
        }
        try (Handle handle = dbi.open()) {
            handle.createStatement("INSERT INTO automation_rules (project, is_active, event_filters, actions, custom_data) VALUES (:project, true, :event_filters, :actions, :custom_data)")
                    .bind("project", project)
                    .bind("event_filters", JsonHelper.encode(rule.scenarios))
                    .bind("custom_data", rule.customData)
                    .bind("actions", JsonHelper.encode(rule.actions)).execute();
        }
        rules.refresh(project);
    }


    public List<AutomationRule> list(String project) {
        return rules.getUnchecked(project);
    }
}
