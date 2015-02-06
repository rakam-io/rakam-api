package org.rakam.plugin.geoip;

import com.google.auto.service.AutoService;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.multibindings.Multibinder;
import org.rakam.plugin.EventMapper;

/**
 * Created by buremba <Burak Emre Kabakcı> on 02/02/15 13:25.
 */
@AutoService(Module.class)
public class GeoIPModule implements Module {
    @Override
    public void configure(Binder binder) {
        Multibinder<EventMapper> eventMappers = Multibinder.newSetBinder(binder, EventMapper.class);
        eventMappers.addBinding().to(GeoIPEventMapper.class);
    }
}
