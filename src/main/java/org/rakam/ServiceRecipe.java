package org.rakam;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import org.rakam.collection.actor.ActorCollectorService;
import org.rakam.collection.event.EventCollectorHttpService;
import org.rakam.plugin.EventMapper;
import org.rakam.plugin.EventProcessor;
import org.rakam.plugin.RakamModule;
import org.rakam.report.ReportAnalyzerService;
import org.rakam.server.http.HttpServerConfig;
import org.rakam.server.http.HttpService;
import org.rakam.stream.ActorCacheAdapter;
import org.rakam.stream.kume.KumeCacheAdapter;

import java.util.ServiceLoader;

import static io.airlift.configuration.ConfigurationModule.bindConfig;

/**
 * Created by buremba on 25/05/14.
 */
public class ServiceRecipe extends AbstractConfigurationAwareModule {
    @Override
    protected void setup(Binder binder) {
        binder.bind(ActorCacheAdapter.class).to(KumeCacheAdapter.class).in(Scopes.SINGLETON);

        Multibinder.newSetBinder(binder, EventProcessor.class);
        Multibinder.newSetBinder(binder, EventMapper.class);

        Multibinder<HttpService> httpServices = Multibinder.newSetBinder(binder, HttpService.class);
        httpServices.addBinding().to(ReportAnalyzerService.class);
        httpServices.addBinding().to(ActorCollectorService.class);
        httpServices.addBinding().to(EventCollectorHttpService.class);

        ServiceLoader<RakamModule> modules = ServiceLoader.load(RakamModule.class);

        Multibinder<RakamModule> rakamModuleBinder = Multibinder.newSetBinder(binder, RakamModule.class);
        for (Module module : modules) {
            if (!(module instanceof RakamModule)) {
                binder.addError("Modules must be subclasses of org.rakam.module.RakamModule: %s", module.getClass().getName());
                continue;
            }
            RakamModule rakamModule = (RakamModule) module;
            super.install(rakamModule);
            rakamModuleBinder.addBinding().toInstance(rakamModule);
        }

        bindConfig(binder).to(HttpServerConfig.class);
    }
}