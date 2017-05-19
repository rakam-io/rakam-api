package org.rakam.analysis;

import com.google.common.collect.ImmutableMap;
import com.google.common.eventbus.Subscribe;
import org.rakam.config.ProjectConfig;
import org.rakam.plugin.MaterializedView;
import org.rakam.plugin.SystemEvents;

import javax.inject.Inject;

import java.time.Duration;

import static java.lang.String.format;
import static org.rakam.util.ValidationUtil.checkCollection;
import static org.rakam.util.ValidationUtil.checkTableColumn;

public class EventExplorerListener
{
    private final MaterializedViewService materializedViewService;
    private final ProjectConfig projectConfig;

    @Inject
    public EventExplorerListener(ProjectConfig projectConfig, MaterializedViewService materializedViewService)
    {
        this.materializedViewService = materializedViewService;
        this.projectConfig = projectConfig;
    }

    @Subscribe
    public void onCreateProject(SystemEvents.ProjectCreatedEvent event)
    {
        createTable(event.project);
    }

    public void createTable(String project)
    {
        String query = format("select date_trunc('hour', %s) as _time, _collection, count(*) as total from _all group by 1, 2",
                checkTableColumn(projectConfig.getTimeColumn()));

        MaterializedView report = new MaterializedView(tableName(),
                format("Event explorer metrics"),
                query,
                Duration.ofHours(1), true, true, ImmutableMap.of());
        materializedViewService.create(project, report).join();
    }

    public static String tableName() {
        return "_event_explorer_metrics";
    }
}
