package org.rakam.ui.report;

import com.google.auto.service.AutoService;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import org.rakam.plugin.RakamModule;
import org.rakam.server.http.HttpService;

@AutoService(RakamModule.class)
public class UIRecipeModule extends RakamModule {

    @Override
    protected void setup(Binder binder) {
        Multibinder<HttpService> httpServices = Multibinder.newSetBinder(binder, HttpService.class);
        httpServices.addBinding().to(UIRecipeHttpService.class).in(Scopes.SINGLETON);
    }

    @Override
    public String name() {
        return "Recipe module";
    }

    @Override
    public String description() {
        return null;
    }
}
