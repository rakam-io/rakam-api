package org.rakam.snapshot;

import com.hazelcast.core.HazelcastInstance;
import org.rakam.cache.hazelcast.HazelcastCacheAdapter;

/**
 * Created by buremba on 29/03/14.
 */
public class HazelcastLock {

    public HazelcastLock() {
        HazelcastInstance adapter = HazelcastCacheAdapter.getAdapter();
    }

    public void lockSystem() {

    }
}
