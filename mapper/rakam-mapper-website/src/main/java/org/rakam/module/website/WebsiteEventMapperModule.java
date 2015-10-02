package org.rakam.module.website;

import com.google.auto.service.AutoService;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import org.rakam.plugin.ConditionalModule;
import org.rakam.plugin.EventMapper;
import org.rakam.plugin.RakamModule;

@AutoService(RakamModule.class)
@ConditionalModule(config = "module.website.mapper", value = "true")
public class WebsiteEventMapperModule extends RakamModule {
    @Override
    protected void setup(Binder binder) {
        Multibinder<EventMapper> eventMappers = Multibinder.newSetBinder(binder, EventMapper.class);
        eventMappers.addBinding().to(UserAgentEventMapper.class).in(Scopes.SINGLETON);
        eventMappers.addBinding().to(ReferrerEventMapper.class).in(Scopes.SINGLETON);
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
