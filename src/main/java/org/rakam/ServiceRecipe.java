package org.rakam;

import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import kafka.javaapi.producer.Producer;
import org.rakam.analysis.DefaultReportDatabase;
import org.rakam.analysis.ReportAnalyzerService;
import org.rakam.collection.actor.ActorCollectorService;
import org.rakam.collection.event.EventCollectorService;
import org.rakam.database.ActorDatabase;
import org.rakam.database.EventDatabase;
import org.rakam.database.ReportDatabase;
import org.rakam.database.rakamdb.DefaultDatabaseAdapter;
import org.rakam.kume.Cluster;
import org.rakam.server.http.HttpService;
import org.rakam.stream.ActorCacheAdapter;
import org.rakam.stream.kume.KumeCacheAdapter;

import java.util.ServiceLoader;

/**
 * Created by buremba on 25/05/14.
 */
public class ServiceRecipe extends AbstractModule {
    private final Cluster cluster;
    private final Producer producer;

    public ServiceRecipe(Cluster cluster, Producer producer) {
        this.cluster = cluster;
        this.producer = producer;
    }

    @Override
    protected void configure() {
        bind(ActorCacheAdapter.class).to(KumeCacheAdapter.class).in(Scopes.SINGLETON);
        bind(EventDatabase.class).to(DefaultDatabaseAdapter.class).in(Scopes.SINGLETON);
        bind(ActorDatabase.class).to(DefaultDatabaseAdapter.class).in(Scopes.SINGLETON);
        bind(ReportDatabase.class).to(DefaultReportDatabase.class).in(Scopes.SINGLETON);
        bind(Cluster.class).toInstance(cluster);
        bind(Producer.class).toInstance(producer);

        Multibinder<HttpService> multibinder = Multibinder.newSetBinder(binder(), HttpService.class);
        multibinder.addBinding().to(ReportAnalyzerService.class);
        multibinder.addBinding().to(ActorCollectorService.class);
        multibinder.addBinding().to(EventCollectorService.class);
//
//        Multibinder<EventMapper> eventMapperBinder
//                = Multibinder.newSetBinder(binder(), EventMapper.class);
//
//        Multibinder<EventProcessor> eventProcessornBinder
//                = Multibinder.newSetBinder(binder(), EventProcessor.class);

        ServiceLoader<Module> modules = ServiceLoader.load(Module.class);

        for (Module module : modules) {
            binder().install(module);
        }

    }
}