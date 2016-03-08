package org.rakam.report.eventexplorer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.eventbus.Subscribe;
import org.rakam.plugin.ContinuousQuery;
import org.rakam.analysis.ContinuousQueryService;
import org.rakam.plugin.SystemEvents;

import javax.inject.Inject;

public class EventExplorerListener {
    private static final String QUERY = "select date_trunc('hour', _time) as time, count(*) as total from \"%s\" group by 1";
    private final ContinuousQueryService continuousQueryService;

    @Inject
    public EventExplorerListener(ContinuousQueryService continuousQueryService) {
        this.continuousQueryService = continuousQueryService;
    }

    @Subscribe
    public void onCreateCollection(SystemEvents.CollectionCreatedEvent event) {
        ContinuousQuery report = new ContinuousQuery(event.project, "Total count of "+event.collection,
                "_total_" + event.collection,
                String.format(QUERY, event.collection),
                ImmutableList.of(), ImmutableMap.of());
        continuousQueryService.create(report, false);
    }
}
