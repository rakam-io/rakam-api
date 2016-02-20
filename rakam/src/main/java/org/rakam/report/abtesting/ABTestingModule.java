package org.rakam.report.abtesting;

import com.google.auto.service.AutoService;
import com.google.inject.Binder;
import com.google.inject.multibindings.Multibinder;
import org.rakam.util.ConditionalModule;
import org.rakam.plugin.RakamModule;
import org.rakam.server.http.HttpService;

@AutoService(RakamModule.class)
@ConditionalModule(config="event.ab-testing.enabled", value = "true")
public class ABTestingModule extends RakamModule {

    @Override
    protected void setup(Binder binder) {
        Multibinder<HttpService> httpServices = Multibinder.newSetBinder(binder, HttpService.class);
        httpServices.addBinding().to(ABTestingHttpService.class);
    }

    @Override
    public String name() {
        return "A/B Testing Module";
    }

    @Override
    public String description() {
        return "A/B Testing with your event data";
    }
}
