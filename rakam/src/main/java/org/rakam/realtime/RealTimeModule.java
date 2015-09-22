package org.rakam.realtime;

import com.google.auto.service.AutoService;
import com.google.inject.Binder;
import com.google.inject.multibindings.Multibinder;
import org.rakam.TimestampEventMapper;
import org.rakam.plugin.ConditionalModule;
import org.rakam.plugin.EventMapper;
import org.rakam.plugin.RakamModule;
import org.rakam.plugin.RealTimeConfig;
import org.rakam.server.http.HttpService;

import static io.airlift.configuration.ConfigurationModule.bindConfig;

/**
 * Created by buremba <Burak Emre Kabakcı> on 02/02/15 13:34.
 */
@AutoService(RakamModule.class)
@ConditionalModule(config = "real-time.enabled", value="true")
public class RealTimeModule extends RakamModule {

    @Override
    protected void setup(Binder binder) {
        bindConfig(binder).to(RealTimeConfig.class);

        Multibinder<HttpService> multiBinder = Multibinder.newSetBinder(binder, HttpService.class);
        multiBinder.addBinding().to(RealTimeHttpService.class);

        Multibinder<EventMapper> mappers = Multibinder.newSetBinder(binder, EventMapper.class);
        mappers.addBinding().to(TimestampEventMapper.class);
    }

    @Override
    public String name() {
        return "Rakam real-time module for time-series data";
    }

    @Override
    public String description() {
        return "Allows you to create real-time dashboards for your events.";
    }
}