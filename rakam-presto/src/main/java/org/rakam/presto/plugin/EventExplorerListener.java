package org.rakam.presto.plugin;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.eventbus.Subscribe;
import org.rakam.analysis.ContinuousQueryService;
import org.rakam.plugin.ContinuousQuery;
import org.rakam.plugin.SystemEvents.ProjectCreatedEvent;
import org.rakam.report.QueryResult;

import javax.inject.Inject;

public class EventExplorerListener {
    private static final String QUERY = "select date_trunc('week', cast(_time as date)) as week, \"$collection\" as collection, date_trunc('hour', _time) as _time,\n" +
            " count(*) as total from _all group by 1, 2, 3";
    private final ContinuousQueryService continuousQueryService;

    @Inject
    public EventExplorerListener(ContinuousQueryService continuousQueryService) {
        this.continuousQueryService = continuousQueryService;
    }

    @Subscribe
    public void onCreateProject(ProjectCreatedEvent event) {
        ContinuousQuery report = new ContinuousQuery("_event_explorer_metrics", QUERY, ImmutableList.of("week"), ImmutableMap.of());
        QueryResult join = continuousQueryService.create(event.project, report, false).getResult().join();
        if(join.isFailed()) {
            throw new IllegalStateException(join.toString());
        }
    }
}
