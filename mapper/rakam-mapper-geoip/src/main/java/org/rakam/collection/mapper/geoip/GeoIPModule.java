package org.rakam.collection.mapper.geoip;

import com.google.inject.Binder;
import com.google.inject.multibindings.Multibinder;
import org.rakam.plugin.ConditionalModule;
import org.rakam.plugin.EventMapper;
import org.rakam.plugin.RakamModule;

import java.io.IOException;

@ConditionalModule(config = "plugin.geoip.enabled", value="true")
public class GeoIPModule extends RakamModule {
    @Override
    protected void setup(Binder binder) {
        GeoIPModuleConfig geoIPModuleConfig = buildConfigObject(GeoIPModuleConfig.class);
        GeoIPEventMapper geoIPEventMapper;
        try {
            geoIPEventMapper = new GeoIPEventMapper(geoIPModuleConfig);
        } catch (IOException e) {
            binder.addError(e);
            return;
        }
        Multibinder<EventMapper> eventMappers = Multibinder.newSetBinder(binder, EventMapper.class);
        eventMappers.addBinding().toInstance(geoIPEventMapper);
    }

    @Override
    public String name() {
        return "GeoIP Event Mapper";
    }

    @Override
    public String description() {
        return "It fills the events that have ip attribute with location information by GeoIP lookup service.";
    }
}
