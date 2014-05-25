package org.rakam;

import com.google.inject.AbstractModule;
import org.rakam.cache.CacheAdapter;
import org.rakam.cache.hazelcast.HazelcastCacheAdapter;
import org.rakam.database.DatabaseAdapter;
import org.rakam.database.cassandra.CassandraAdapter;

/**
 * Created by buremba on 25/05/14.
 */
public class ServiceRecipe extends AbstractModule {
    @Override
    protected void configure() {
        bind(DatabaseAdapter.class).to(CassandraAdapter.class);
        bind(CacheAdapter.class).to(HazelcastCacheAdapter.class);
    }
}