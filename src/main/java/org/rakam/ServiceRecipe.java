package org.rakam;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import org.rakam.analysis.DefaultAnalysisRuleDatabase;
import org.rakam.database.ActorDatabase;
import org.rakam.database.AnalysisRuleDatabase;
import org.rakam.database.EventDatabase;
import org.rakam.database.rakamdb.DefaultDatabaseAdapter;
import org.rakam.kume.Cluster;
import org.rakam.plugin.CollectionMapperPlugin;
import org.rakam.stream.ActorCacheAdapter;
import org.rakam.stream.kume.KumeCacheAdapter;
import org.rakam.util.json.JsonArray;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by buremba on 25/05/14.
 */
public class ServiceRecipe extends AbstractModule {
    private final Cluster cluster;
    JsonArray plugins;
    Logger logger = Logger.getLogger(this.getClass().getName());

    public ServiceRecipe(Cluster cluster) {
        this.cluster = cluster;
    }

    public ServiceRecipe(Cluster cluster, JsonArray plugins) {
        this.cluster = cluster;
        this.plugins = plugins;
    }

    @Override
    protected void configure() {
        bind(ActorCacheAdapter.class).to(KumeCacheAdapter.class);
        bind(EventDatabase.class).to(DefaultDatabaseAdapter.class);
        bind(ActorDatabase.class).to(DefaultDatabaseAdapter.class);
        bind(AnalysisRuleDatabase.class).to(DefaultAnalysisRuleDatabase.class);

        bind(KumeCacheAdapter.class)
                .toProvider(() -> new KumeCacheAdapter(cluster))
                .in(Scopes.SINGLETON);
        bind(DefaultDatabaseAdapter.class)
                .toProvider(() -> new DefaultDatabaseAdapter(cluster))
                .in(Scopes.SINGLETON);
        bind(DefaultAnalysisRuleDatabase.class)
                .toProvider(() -> new DefaultAnalysisRuleDatabase(cluster))
                .in(Scopes.SINGLETON);

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