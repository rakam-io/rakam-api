package org.rakam.plugin;

import com.google.auto.service.AutoService;
import com.google.inject.Binder;
import com.google.inject.multibindings.Multibinder;
import org.rakam.plugin.tasks.LockService;
import org.rakam.plugin.tasks.ScheduledTaskHttpService;
import org.rakam.server.http.HttpService;
import org.rakam.util.ConditionalModule;

@AutoService(RakamModule.class)
@ConditionalModule(config = "tasks.enable", value = "true")
public class ScheduledTaskModule extends RakamModule
{
    @Override
    protected void setup(Binder binder)
    {
        Multibinder<HttpService> httpServices = Multibinder.newSetBinder(binder, HttpService.class);
        httpServices.addBinding().to(ScheduledTaskHttpService.class);

        binder.bind(LockService.class).toProvider(LockServiceProvider.class);
    }

    @Override
    public String name()
    {
        return null;
    }

    @Override
    public String description()
    {
        return null;
    }
}
