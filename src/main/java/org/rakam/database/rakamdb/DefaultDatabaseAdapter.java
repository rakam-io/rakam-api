package org.rakam.database.rakamdb;

import com.google.inject.Inject;
import org.rakam.kume.Cluster;
import org.rakam.util.Interval;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by buremba <Burak Emre Kabakcı> on 02/01/15 14:12.
 */
public class DefaultDatabaseAdapter {
    private final Cluster cluster;
    Map<String, RakamDB> dbs = new ConcurrentHashMap<>();

    @Inject
    public DefaultDatabaseAdapter(Cluster cluster) {
        this.cluster = cluster;
    }


    RakamDB getDBforProject(String projectId) {
        RakamDB rakamDB = dbs.get(projectId);
        if(rakamDB==null) {
            RakamDB db = cluster.createOrGetService("rakamdb_" + projectId, bus -> new RakamDB(bus, Interval.DAY, 2));
            dbs.put(projectId, db);
            return db;
        } else {
            return rakamDB;
        }
    }
}
