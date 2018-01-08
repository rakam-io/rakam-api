package org.rakam.plugin;

import com.google.auto.service.AutoService;
import com.google.inject.Binder;
import com.google.inject.multibindings.Multibinder;
import org.rakam.collection.WebHookHttpService;
import org.rakam.config.TaskConfig;
import org.rakam.server.http.HttpService;
import org.rakam.util.ConditionalModule;

import static io.airlift.configuration.ConfigBinder.configBinder;

@AutoService(RakamModule.class)
@ConditionalModule(config = "webhook.enable", value = "true")
public class WebhookModule
        extends RakamModule {
    @Override
    protected void setup(Binder binder) {
        configBinder(binder).bindConfig(TaskConfig.class);
        Multibinder<HttpService> httpServices = Multibinder.newSetBinder(binder, HttpService.class);
        httpServices.addBinding().to(WebHookHttpService.class);
    }

    @Override
    public String name() {
        return null;
    }

    @Override
    public String description() {
        return null;
    }
}

