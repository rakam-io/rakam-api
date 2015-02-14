package org.rakam;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import org.rakam.collection.actor.ActorCollectorService;
import org.rakam.collection.event.EventCollectorService;
import org.rakam.database.ActorDatabase;
import org.rakam.database.EventDatabase;
import org.rakam.database.rakamdb.DefaultDatabaseAdapter;
import org.rakam.kume.Cluster;
import org.rakam.plugin.EventMapper;
import org.rakam.plugin.EventProcessor;
import org.rakam.plugin.RakamModule;
import org.rakam.report.ReportAnalyzerService;
import org.rakam.server.http.HttpService;
import org.rakam.stream.ActorCacheAdapter;
import org.rakam.stream.kume.KumeCacheAdapter;

import java.util.ServiceLoader;

/**
 * Created by buremba on 25/05/14.
 */
public class ServiceRecipe extends RakamModule {
    private final Cluster cluster;

    public ServiceRecipe(Cluster cluster) {
        this.cluster = cluster;
    }

    @Override
    protected void setup(Binder binder) {
        binder.bind(ActorCacheAdapter.class).to(KumeCacheAdapter.class).in(Scopes.SINGLETON);
        binder.bind(EventDatabase.class).to(DefaultDatabaseAdapter.class).in(Scopes.SINGLETON);
        binder.bind(ActorDatabase.class).to(DefaultDatabaseAdapter.class).in(Scopes.SINGLETON);

        binder.bind(Cluster.class).toInstance(cluster);

        Multibinder.newSetBinder(binder, EventProcessor.class);
        Multibinder.newSetBinder(binder, EventMapper.class);

        Multibinder<HttpService> httpServices = Multibinder.newSetBinder(binder, HttpService.class);
        httpServices.addBinding().to(ReportAnalyzerService.class);
        httpServices.addBinding().to(ActorCollectorService.class);
        httpServices.addBinding().to(EventCollectorService.class);

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
    }

    @Override
    public String name() {
        return null;
    }

    @Override
    public String description() {
        return null;
    }

    @Override
    public void onDestroy() {

    }
}