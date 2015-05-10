package org.rakam.ui;

import com.google.inject.Binder;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Names;
import org.rakam.plugin.RakamModule;
import org.rakam.server.http.HttpService;

import static io.airlift.configuration.ConfigurationModule.bindConfig;

/**
 * Created by buremba <Burak Emre Kabakcı> on 16/04/15 21:21.
 */
public class RakamUIModule extends RakamModule {
    @Override
    protected void setup(Binder binder) {
        bindConfig(binder)
                .annotatedWith(Names.named("report.metadata.store.jdbc"))
                .prefixedWith("report.metadata.store.jdbc")
                .to(JDBCConfig.class);

        Multibinder<HttpService> httpServices = Multibinder.newSetBinder(binder, HttpService.class);
        httpServices.addBinding().to(ReportHttpService.class);
        httpServices.addBinding().to(RakamUIWebService.class);
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
