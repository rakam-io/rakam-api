package org.rakam.analysis.realtime;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.swagger.models.Tag;
import org.rakam.config.MetadataConfig;
import org.rakam.plugin.EventMapper;
import org.rakam.plugin.RakamModule;
import org.rakam.plugin.TimestampEventMapper;
import org.rakam.report.realtime.RealTimeConfig;
import org.rakam.server.http.HttpService;

import static io.airlift.configuration.ConfigurationModule.bindConfig;

//@AutoService(RakamModule.class)
//@ConditionalModule(config = "real-time.enabled", value="true")
public class RealTimeModule extends RakamModule {

    @Override
    protected void setup(Binder binder) {
        bindConfig(binder).to(RealTimeConfig.class);

        Multibinder<HttpService> multiBinder = Multibinder.newSetBinder(binder, HttpService.class);
        multiBinder.addBinding().to(RealTimeHttpService.class);

        Multibinder<EventMapper> mappers = Multibinder.newSetBinder(binder, EventMapper.class);
        mappers.permitDuplicates().addBinding().to(TimestampEventMapper.class);
        mappers.addBinding().to(RealtimeEventProcessor.class).in(Scopes.SINGLETON);

        Multibinder.newSetBinder(binder, Tag.class).addBinding()
                .toInstance(new Tag().name("realtime").description("Realtime")
                        .externalDocs(MetadataConfig.centralDocs));
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