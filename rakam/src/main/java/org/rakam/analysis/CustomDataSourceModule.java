package org.rakam.analysis;

import com.google.auto.service.AutoService;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import org.rakam.analysis.datasource.CustomDataSourceHttpService;
import org.rakam.plugin.RakamModule;
import org.rakam.analysis.datasource.CustomDataSourceService;
import org.rakam.server.http.HttpService;
import org.rakam.util.ConditionalModule;

@AutoService(RakamModule.class)
@ConditionalModule(config="custom-data-source.enabled", value = "true")
public class CustomDataSourceModule extends RakamModule
{
    @Override
    protected void setup(Binder binder)
    {
        Multibinder<HttpService> httpServiceMultibinder = Multibinder.newSetBinder(binder, HttpService.class);
        httpServiceMultibinder.addBinding().to(CustomDataSourceHttpService.class).in(Scopes.SINGLETON);
    }

    @Override
    public String name()
    {
        return "Custom data source module";
    }

    @Override
    public String description()
    {
        return null;
    }
}
