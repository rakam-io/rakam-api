package org.rakam;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import org.rakam.stream.ActorCacheAdapter;
import org.rakam.stream.MetricStreamAdapter;
import org.rakam.stream.TimeSeriesStreamAdapter;
import org.rakam.stream.kume.KumeStreamAdapter;
import org.rakam.stream.local.LocalCache;
import org.rakam.stream.local.LocalCacheImpl;
import org.rakam.database.AnalysisRuleDatabase;
import org.rakam.database.DatabaseAdapter;
import org.rakam.database.cassandra.CassandraAdapter;
import org.rakam.plugin.CollectionMapperPlugin;
import org.rakam.util.json.JsonArray;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by buremba on 25/05/14.
 */
public class ServiceRecipe extends AbstractModule {
    JsonArray plugins;
    Logger logger = Logger.getLogger(this.getClass().getName());

    public ServiceRecipe() {
    }

    public ServiceRecipe(JsonArray plugins) {
        this.plugins = plugins;
    }

    @Override
    protected void configure() {
        bind(LocalCache.class).to(LocalCacheImpl.class);
        bind(LocalCacheImpl.class).in(Scopes.SINGLETON);

        bind(ActorCacheAdapter.class).to(KumeStreamAdapter.class);
        bind(MetricStreamAdapter.class).to(KumeStreamAdapter.class);
        bind(TimeSeriesStreamAdapter.class).to(KumeStreamAdapter.class);
        bind(KumeStreamAdapter.class).in(Scopes.SINGLETON);

        bind(DatabaseAdapter.class).to(CassandraAdapter.class);
        bind(AnalysisRuleDatabase.class).to(CassandraAdapter.class);

        if (plugins != null)
            for (Object plugin : plugins) {
                if (plugin instanceof String) {
                    Class<?> clazz;
                    try {
                        clazz = Class.forName((String) plugin);
                    } catch (ClassNotFoundException e) {
                        logger.log(Level.WARNING, "plugin class couldn't found", e);
                        continue;
                    }
                    for (Class c : clazz.getInterfaces()) {
                        String clazz_name = c.getName();
                        if (clazz_name.equals(CollectionMapperPlugin.class.getName())) {
                            bind(new TypeLiteral<CollectionMapperPlugin>() {
                            })
                                    .to(clazz.asSubclass(CollectionMapperPlugin.class));
                            continue;
                        }

                    }
                    logger.warning(plugin + " couldn't added as plugin.");
                }
            }
    }
}