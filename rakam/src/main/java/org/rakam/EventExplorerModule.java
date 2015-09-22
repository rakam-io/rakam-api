package org.rakam;

import com.google.auto.service.AutoService;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import org.rakam.analysis.EventExplorerHttpService;
import org.rakam.plugin.ConditionalModule;
import org.rakam.plugin.EventMapper;
import org.rakam.plugin.RakamModule;
import org.rakam.server.http.HttpService;

/**
 * Created by buremba <Burak Emre Kabakcı> on 15/07/15 12:29.
 */
@AutoService(RakamModule.class)
@ConditionalModule(config = "event-explorer.enabled", value = "true")
public class EventExplorerModule extends RakamModule {
    @Override
    protected void setup(Binder binder) {
        Multibinder<HttpService> httpServices = Multibinder.newSetBinder(binder, HttpService.class);
        httpServices.addBinding().to(EventExplorerHttpService.class);

        Multibinder<EventMapper> timeMapper = Multibinder.newSetBinder(binder, EventMapper.class);
        timeMapper.addBinding().to(TimestampEventMapper.class).in(Scopes.SINGLETON);
    }

    @Override
    public String name() {
        return null;
    }

    @Override
    public String description() {
        return null;
    }
}
