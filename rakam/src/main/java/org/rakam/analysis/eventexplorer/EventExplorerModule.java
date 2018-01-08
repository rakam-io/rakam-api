package org.rakam.analysis.eventexplorer;

import com.google.auto.service.AutoService;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.swagger.models.Tag;
import org.rakam.analysis.EventExplorerListener;
import org.rakam.config.MetadataConfig;
import org.rakam.plugin.EventMapper;
import org.rakam.plugin.RakamModule;
import org.rakam.plugin.TimestampEventMapper;
import org.rakam.server.http.HttpService;
import org.rakam.util.ConditionalModule;

@AutoService(RakamModule.class)
@ConditionalModule(config = "event-explorer.enabled", value = "true")
public class EventExplorerModule
        extends RakamModule {
    @Override
    protected void setup(Binder binder) {
        Multibinder<HttpService> httpServices = Multibinder.newSetBinder(binder, HttpService.class);
        httpServices.addBinding().to(EventExplorerHttpService.class);

        Multibinder<EventMapper> timeMapper = Multibinder.newSetBinder(binder, EventMapper.class);
        timeMapper.permitDuplicates().addBinding().to(TimestampEventMapper.class).in(Scopes.SINGLETON);

        Multibinder<Tag> tags = Multibinder.newSetBinder(binder, Tag.class);
        tags.addBinding().toInstance(new Tag().name("event-explorer").description("Event Explorer").externalDocs(MetadataConfig.centralDocs));

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
}
