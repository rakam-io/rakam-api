package org.rakam.plugin;

import com.google.auto.service.AutoService;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import org.rakam.server.http.HttpService;
import org.rakam.util.ConditionalModule;

@AutoService(RakamModule.class)
@ConditionalModule(config = "js-event-mapper.enabled", value = "true")
public class JSEventMapperModule extends RakamModule {
    @Override
    protected void setup(Binder binder) {
        Multibinder<HttpService> httpServices = Multibinder.newSetBinder(binder, HttpService.class);
        httpServices.addBinding().to(CustomEventMapperHttpService.class).in(Scopes.SINGLETON);
    }

    @Override
    public String name() {
        return "Custom event mapper module";
    }

    @Override
    public String description() {
        return "Write Javascript code to enrich and sanitize your event data in real-time";
    }
}
