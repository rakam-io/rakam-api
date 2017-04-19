package org.rakam.presto.analysis;

import com.google.common.collect.ImmutableSet;
import com.google.common.eventbus.EventBus;
import com.google.inject.name.Named;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.collection.SchemaField;
import org.rakam.config.ProjectConfig;
import org.rakam.util.NotExistsException;
import org.rakam.util.RakamException;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.util.StringMapper;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import java.util.List;
import java.util.Set;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_IMPLEMENTED;
import static org.rakam.util.ValidationUtil.checkProject;

public class PrestoMetastore
        extends PrestoAbstractMetastore
{
    private final DBI dbi;

    @Inject
    public PrestoMetastore(ProjectConfig projectConfig, @Named("report.metadata.store.jdbc") JDBCPoolDataSource dataSource, PrestoConfig config, EventBus eventBus)
    {
        super(projectConfig, config, eventBus);
        dbi = new DBI(dataSource);
    }

    @PostConstruct
    public void setup()
    {
        dbi.inTransaction((Handle handle, TransactionStatus transactionStatus) -> {
            handle.createStatement("CREATE TABLE IF NOT EXISTS project (" +
                    "  name TEXT NOT NULL,\n" +
                    "  PRIMARY KEY (name))")
                    .execute();
            return null;
        });
    }

    @Override
    public void createProject(String project)
    {
        if (config.getExistingProjects().contains(project)) {
            checkProject(project);

            try (Handle handle = dbi.open()) {
                handle.createStatement("INSERT INTO project (name) VALUES(:name)")
                        .bind("name", project)
                        .execute();
            }
            catch (Exception e) {
                if (getProjects().contains(project)) {
                    throw new RakamException("The project already exists", BAD_REQUEST);
                }
            }

            super.onCreateProject(project);
            return;
        }

        throw new RakamException(NOT_IMPLEMENTED);
    }

    @Override
    public Set<String> getProjects()
    {
        try (Handle handle = dbi.open()) {
            return ImmutableSet.copyOf(
                    handle.createQuery("select name from project")
                            .map(StringMapper.FIRST).iterator());
        }
    }

    @Override
    public void deleteProject(String project)
    {
        try (Handle handle = dbi.open()) {
            handle.createStatement("delete from project where name = :project")
                    .bind("project", project).execute();
        }
    }

    @Override
    public synchronized List<SchemaField> getOrCreateCollectionFields(String project, String collection, Set<SchemaField> fields)
            throws NotExistsException
    {
        throw new RakamException("Presto is read-only", BAD_REQUEST);
    }
}
