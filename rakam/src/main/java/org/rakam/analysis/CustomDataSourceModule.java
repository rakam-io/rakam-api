package org.rakam.analysis;

import com.google.auto.service.AutoService;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.multibindings.OptionalBinder;
import org.rakam.analysis.datasource.CustomDataSourceConfig;
import org.rakam.analysis.datasource.CustomDataSourceHttpService;
import org.rakam.analysis.datasource.CustomDataSourceService;
import org.rakam.plugin.RakamModule;
import org.rakam.server.http.HttpService;

@AutoService(RakamModule.class)
public class CustomDataSourceModule extends RakamModule {
    @Override
    protected void setup(Binder binder) {
        CustomDataSourceConfig config = buildConfigObject(CustomDataSourceConfig.class);
        OptionalBinder.newOptionalBinder(binder, CustomDataSourceService.class);

        if (config.getEnabled()) {
            Multibinder<HttpService> httpServiceMultibinder = Multibinder.newSetBinder(binder, HttpService.class);
            httpServiceMultibinder.addBinding().to(CustomDataSourceHttpService.class).in(Scopes.SINGLETON);
            binder.bind(CustomDataSourceService.class);
        }
    }

    @Override
    public String name() {
        return "Custom data source module";
    }

    @Override
    public String description() {
        return null;
    }
}
