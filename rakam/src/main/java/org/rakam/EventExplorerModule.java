package org.rakam;

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.eventbus.Subscribe;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import org.rakam.analysis.EventExplorerHttpService;
import org.rakam.plugin.ConditionalModule;
import org.rakam.plugin.ContinuousQuery;
import org.rakam.plugin.ContinuousQueryService;
import org.rakam.plugin.EventMapper;
import org.rakam.plugin.RakamModule;
import org.rakam.plugin.SystemEvents;
import org.rakam.plugin.TimestampEventMapper;
import org.rakam.server.http.HttpService;

import javax.inject.Inject;

@AutoService(RakamModule.class)
@ConditionalModule(config = "event-explorer.enabled", value = "true")
public class EventExplorerModule extends RakamModule {
    @Override
    protected void setup(Binder binder) {
        Multibinder<HttpService> httpServices = Multibinder.newSetBinder(binder, HttpService.class);
        httpServices.addBinding().to(EventExplorerHttpService.class);

        Multibinder<EventMapper> timeMapper = Multibinder.newSetBinder(binder, EventMapper.class);
        timeMapper.permitDuplicates().addBinding().to(TimestampEventMapper.class).in(Scopes.SINGLETON);

        binder.bind(EventExplorerListener.class).asEagerSingleton();
    }

    @Override
    public String name() {
        return "Event Explorer Module";
    }

    @Override
    public String description() {
        return "It allows analyzing and visualizing events via a simple interface.";
    }

    private static class EventExplorerListener {
        private static final String QUERY = "select _time/3600 as time, count(*) as total from \"%s\" group by 1";
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
            continuousQueryService.create(report);
        }
    }
}
