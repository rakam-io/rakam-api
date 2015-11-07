package org.rakam.automation;


import com.google.auto.service.AutoService;
import com.google.inject.Binder;
import com.google.inject.multibindings.Multibinder;
import org.rakam.plugin.ConditionalModule;
import org.rakam.plugin.EventMapper;
import org.rakam.plugin.RakamModule;
import org.rakam.server.http.HttpService;

@AutoService(RakamModule.class)
@ConditionalModule(config = "automation.enabled", value = "true")
public class AutomationModule extends RakamModule {
    @Override
    protected void setup(Binder binder) {
        Multibinder<EventMapper> eventMappers = Multibinder.newSetBinder(binder, EventMapper.class);
        eventMappers.addBinding().to(AutomationEventMapper.class);

        Multibinder<HttpService> httpServices = Multibinder.newSetBinder(binder, HttpService.class);
        httpServices.addBinding().to(AutomationHttpService.class);
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
