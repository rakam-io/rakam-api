package org.rakam.plugin.realtime;

import com.google.auto.service.AutoService;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.multibindings.Multibinder;
import org.rakam.plugin.EventProcessor;

/**
 * Created by buremba <Burak Emre Kabakcı> on 02/02/15 13:34.
 */
@AutoService(Module.class)
public class RealTimeModule implements Module {
    @Override
    public void configure(Binder binder) {
        Multibinder<EventProcessor> eventMappers = Multibinder.newSetBinder(binder, EventProcessor.class);
        eventMappers.addBinding().to(RealTimeEventProcessor.class);
    }
}