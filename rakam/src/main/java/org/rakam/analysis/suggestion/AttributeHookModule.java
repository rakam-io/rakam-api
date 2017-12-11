package org.rakam.analysis.suggestion;

import com.google.auto.service.AutoService;
import com.google.inject.Binder;
import com.google.inject.multibindings.Multibinder;
import org.rakam.plugin.RakamModule;
import org.rakam.server.http.HttpService;

@AutoService(RakamModule.class)
public class AttributeHookModule extends RakamModule {
    @Override
    protected void setup(Binder binder) {
        Multibinder<HttpService> httpServices = Multibinder.newSetBinder(binder, HttpService.class);
        httpServices.addBinding().to(AutoSuggestionHttpService.class);

        binder.bind(AttributeHook.class).asEagerSingleton();
    }

    @Override
    public String name() {
        return "Suggestion Module";
    }

    @Override
    public String description() {
        return "It provides auto-suggestion for attributes";
    }
}
