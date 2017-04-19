package org.rakam.presto.plugin;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.eventbus.Subscribe;
import org.rakam.analysis.ContinuousQueryService;
import org.rakam.config.ProjectConfig;
import org.rakam.plugin.ContinuousQuery;
import org.rakam.plugin.SystemEvents.ProjectCreatedEvent;
import org.rakam.report.QueryResult;

import javax.inject.Inject;

import static org.rakam.util.ValidationUtil.checkTableColumn;

public class EventExplorerListener {
    private final ContinuousQueryService continuousQueryService;
    private final ProjectConfig projectConfig;

    @Inject
    public EventExplorerListener(ProjectConfig projectConfig, ContinuousQueryService continuousQueryService) {
        this.continuousQueryService = continuousQueryService;
        this.projectConfig = projectConfig;
    }

    @Subscribe
    public void onCreateProject(ProjectCreatedEvent event) {
        String query = String.format("select date_trunc('week', cast(%s as date)) as week, \"$collection\" as collection, date_trunc('hour', %s) as %s,\n" +
                " count(*) as total from _all group by 1, 2, 3", checkTableColumn(projectConfig.getTimeColumn()),
                checkTableColumn(projectConfig.getTimeColumn()), checkTableColumn(projectConfig.getTimeColumn()));

        ContinuousQuery report = new ContinuousQuery("_event_explorer_metrics", "Event explorer metrics", query, ImmutableList.of("week"), ImmutableMap.of());
        QueryResult join = continuousQueryService.create(event.project, report, false).getResult().join();
        if(join.isFailed()) {
            throw new IllegalStateException(join.toString());
        }
    }
}
