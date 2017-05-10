package org.rakam.analysis;

import com.google.common.collect.ImmutableMap;
import com.google.common.eventbus.Subscribe;
import org.rakam.analysis.MaterializedViewService;
import org.rakam.config.ProjectConfig;
import org.rakam.plugin.MaterializedView;
import org.rakam.plugin.SystemEvents.CollectionCreatedEvent;

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
    public void onCreateCollection(CollectionCreatedEvent event)
    {
        createTable(event.project, event.collection);
    }

    public void createTable(String project, String collection)
    {
        String query = format("select date_trunc('hour', %s) as _time, count(*) as total from %s group by 1",
                checkTableColumn(projectConfig.getTimeColumn()), checkCollection(collection));

        MaterializedView report = new MaterializedView("_event_explorer_metrics - " + collection,
                format("Event explorer metrics for %s collection", collection),
                query,
                Duration.ofHours(1), true, true, ImmutableMap.of());
        materializedViewService.create(project, report).join();
    }
}
