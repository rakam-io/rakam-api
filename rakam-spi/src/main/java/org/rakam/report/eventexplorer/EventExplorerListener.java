package org.rakam.report.eventexplorer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.eventbus.Subscribe;
import org.rakam.plugin.ContinuousQuery;
import org.rakam.analysis.ContinuousQueryService;
import org.rakam.plugin.SystemEvents;

import javax.inject.Inject;

public class EventExplorerListener {
    private static final String QUERY = "select date_trunc('week', cast(_time as date)) as week, date_trunc('hour', _time) as _time, " +
            "count(*) as total from \"%s\" group by 1, 2";
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
                ImmutableList.of("week"), ImmutableMap.of());
        continuousQueryService.create(report, false);
    }
}
