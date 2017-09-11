package org.rakam.automation;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import org.rakam.config.EncryptionConfig;
import org.rakam.plugin.EventMapper;
import org.rakam.plugin.RakamModule;
import org.rakam.plugin.user.UserActionService;
import org.rakam.server.http.HttpService;
import org.rakam.util.ConditionalModule;

import static io.airlift.configuration.ConfigBinder.configBinder;

@ConditionalModule(config = "automation.enabled", value = "true")
public class AutomationModule extends RakamModule {
    @Override
    protected void setup(Binder binder) {
        configBinder(binder).bindConfig(EncryptionConfig.class);
        Multibinder<EventMapper> eventProcessors = Multibinder.newSetBinder(binder, EventMapper.class);
        eventProcessors.addBinding().to(AutomationEventProcessor.class).in(Scopes.SINGLETON);
        Multibinder.newSetBinder(binder, UserActionService.class);

        binder.bind(UserAutomationService.class);

        Multibinder<AutomationAction> automationActions = Multibinder.newSetBinder(binder, AutomationAction.class);
        for (AutomationActionType automationActionType : AutomationActionType.values()) {
            automationActions.addBinding().to(automationActionType.getActionClass());
        }

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
