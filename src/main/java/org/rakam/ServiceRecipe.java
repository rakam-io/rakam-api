package org.rakam;

import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;
import org.rakam.cache.DistributedCacheAdapter;
import org.rakam.cache.hazelcast.HazelcastCacheAdapter;
import org.rakam.database.AnalysisRuleDatabase;
import org.rakam.database.DatabaseAdapter;
import org.rakam.database.cassandra.CassandraAdapter;
import org.rakam.plugin.CollectionMapperPlugin;
import org.vertx.java.core.json.JsonArray;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by buremba on 25/05/14.
 */
public class ServiceRecipe extends AbstractModule {
    JsonArray plugins;
    Logger logger = Logger.getLogger(this.getClass().getName());

    public ServiceRecipe(JsonArray plugins) {
        this.plugins = plugins;
    }

    @Override
    protected void configure() {
        bind(DatabaseAdapter.class).to(CassandraAdapter.class);
        bind(DistributedCacheAdapter.class).to(HazelcastCacheAdapter.class);
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
                            bind(new TypeLiteral<CollectionMapperPlugin>() {}).to(clazz.asSubclass(CollectionMapperPlugin.class));
                        } else
                            logger.warning(plugin+" couldn't added as plugin.");
                    }


                }
            }
    }
}