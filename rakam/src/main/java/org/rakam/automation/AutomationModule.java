package org.rakam.automation;


import com.google.auto.service.AutoService;
import com.google.inject.Binder;
import com.google.inject.multibindings.Multibinder;
import org.rakam.plugin.ConditionalModule;
import org.rakam.plugin.EventMapper;
import org.rakam.plugin.RakamModule;

@AutoService(RakamModule.class)
@ConditionalModule(config = "automation.enabled", value = "true")
public class AutomationModule extends RakamModule {
    @Override
    protected void setup(Binder binder) {
        Multibinder<EventMapper> eventMappers = Multibinder.newSetBinder(binder, EventMapper.class);
        eventMappers.addBinding().to(AutomationEventMapper.class);
    }

    @Override
    public String name() {
        return "Automation Module";
    }

    @Override
    public String description() {
        return "Take action based on events";
    }
}
