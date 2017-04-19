package org.rakam.presto.analysis;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.eventbus.EventBus;
import com.google.inject.name.Named;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.config.ProjectConfig;
import org.rakam.util.ProjectCollection;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.util.StringMapper;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;

import static org.rakam.util.ValidationUtil.checkProject;

public class PrestoRaptorMetastore
        extends PrestoAbstractMetastore
{
    private final DBI dbi;

    @Inject
    public PrestoRaptorMetastore(ProjectConfig projectConfig, @Named("presto.metastore.jdbc") JDBCPoolDataSource dataSource, PrestoConfig config, EventBus eventBus)
    {
        super(projectConfig, config, eventBus);
        dbi = new DBI(dataSource);
    }

    @PostConstruct
    public void setup() {
        dbi.inTransaction((Handle handle, TransactionStatus transactionStatus) -> {
            handle.createStatement("CREATE TABLE IF NOT EXISTS project (" +
                    "  name TEXT NOT NULL,\n" +
                    "  PRIMARY KEY (name))")
                    .execute();
            return null;
        });
    }

    @Override
    public void createProject(String project) {
        checkProject(project);

        try (Handle handle = dbi.open()) {
            handle.createStatement("INSERT INTO project (name) VALUES(:name)")
                    .bind("name", project)
                    .execute();
        }

        super.onCreateProject(project);
    }

    @Override
    public Set<String> getProjects() {
        try (Handle handle = dbi.open()) {
            return ImmutableSet.copyOf(
                    handle.createQuery("select name from project")
                            .map(StringMapper.FIRST).iterator());
        }
    }

    @Override
    public void deleteProject(String project) {
        try (Handle handle = dbi.open()) {
            handle.createStatement("delete from project where name = :project")
                    .bind("project", project).execute();
        }

        Set<String> collectionNames = getCollectionNames(project);
        try (Connection connection = prestoConnectionFactory.openConnection()) {
            Statement statement = connection.createStatement();

            while (!collectionNames.isEmpty()) {
                for (String collectionName : collectionNames) {
                    statement.execute(String.format("drop table %s.%s.%s",
                            config.getColdStorageConnector(), project, collectionName));
                    schemaCache.invalidate(new ProjectCollection(project, collectionName));
                }

                collectionCache.refresh(project);
                collectionNames = collectionCache.getUnchecked(project);
            }
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }

        collectionCache.invalidate(project);
        super.onDeleteProject(project);
    }
}
