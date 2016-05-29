package org.rakam.module.website;

import com.google.auto.service.AutoService;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import org.rakam.util.ConditionalModule;
import org.rakam.plugin.EventMapper;
import org.rakam.plugin.RakamModule;
import org.rakam.plugin.user.UserPropertyMapper;

@AutoService(RakamModule.class)
@ConditionalModule(config = "module.website.mapper", value = "true")
public class WebsiteEventMapperModule extends RakamModule {
    @Override
    protected void setup(Binder binder) {
        Multibinder<UserPropertyMapper> userPropertyMappers = Multibinder.newSetBinder(binder, UserPropertyMapper.class);
        Multibinder<EventMapper> eventMappers = Multibinder.newSetBinder(binder, EventMapper.class);

        WebsiteMapperConfig config = buildConfigObject(WebsiteMapperConfig.class);
        if(config.getReferrer()) {
            eventMappers.addBinding().to(ReferrerEventMapper.class).in(Scopes.SINGLETON);
            userPropertyMappers.addBinding().to(ReferrerEventMapper.class).in(Scopes.SINGLETON);
        }
        if(config.getUserAgent()) {
            eventMappers.addBinding().to(UserAgentEventMapper.class).in(Scopes.SINGLETON);
            userPropertyMappers.addBinding().to(UserAgentEventMapper.class).in(Scopes.SINGLETON);
        }

        Multibinder.newSetBinder(binder, EventMapper.class).addBinding()
                .to(UserIdEventMapper.class).in(Scopes.SINGLETON);
    }

    @Override
    public String name() {
        return "Event website related attribute mapping module";
    }

    @Override
    public String description() {
        return "Resolves _referrer, _user_agent attributes and related fields such as user_agent_version, referrer_medium to the event.";
    }
}
