package org.rakam.analysis.webhook;


import com.google.auto.service.AutoService;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import org.rakam.aws.AWSConfig;
import org.rakam.plugin.EventMapper;
import org.rakam.plugin.RakamModule;

import static io.airlift.configuration.ConfigBinder.configBinder;

@AutoService(RakamModule.class)
public class WebhookModule extends RakamModule {
    @Override
    protected void setup(Binder binder) {
        WebhookConfig webhookConfig = buildConfigObject(WebhookConfig.class);
        if(webhookConfig.getUrl() != null) {
            configBinder(binder).bindConfig(WebhookConfig.class);
            configBinder(binder).bindConfig(AWSConfig.class);
            Multibinder<EventMapper> mappers = Multibinder.newSetBinder(binder, EventMapper.class);
            mappers.addBinding().to(WebhookEventMapper.class).in(Scopes.SINGLETON);
        }
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
