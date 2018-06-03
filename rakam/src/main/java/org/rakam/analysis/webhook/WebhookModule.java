package org.rakam.analysis.webhook;


import com.google.auto.service.AutoService;
import com.google.inject.Binder;
import com.google.inject.multibindings.Multibinder;
import org.rakam.plugin.EventMapper;
import org.rakam.plugin.RakamModule;
import org.rakam.util.ConditionalModule;

import static io.airlift.configuration.ConfigBinder.configBinder;

@AutoService(RakamModule.class)
@ConditionalModule(config = "collection.webhook.url")
public class WebhookModule extends RakamModule {
    @Override
    protected void setup(Binder binder) {
        configBinder(binder).bindConfig(WebhookConfig.class);
        Multibinder<EventMapper> mappers = Multibinder.newSetBinder(binder, EventMapper.class);
        mappers.addBinding().to(WebhookEventMapper.class);
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
