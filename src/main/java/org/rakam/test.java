package org.rakam;

import com.google.inject.AbstractModule;
import org.rakam.cache.SimpleCacheAdapter;
import org.rakam.cache.hazelcast.HazelcastCacheAdapter;
import org.rakam.database.DatabaseAdapter;
import org.rakam.database.cassandra.CassandraAdapter;

/**
 * Created by buremba on 12/05/14.
 */
public class test extends AbstractModule {
    @Override
    protected void configure() {
        bind(DatabaseAdapter.class).to(CassandraAdapter.class);
        bind(SimpleCacheAdapter.class).to(HazelcastCacheAdapter.class);
    }
}