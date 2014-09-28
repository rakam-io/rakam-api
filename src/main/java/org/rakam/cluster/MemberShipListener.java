package org.rakam.cluster;

import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.instance.HazelcastInstanceProxy;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.rakam.ServiceStarter;
import org.rakam.cache.DistributedCacheAdapter;
import org.rakam.database.DatabaseAdapter;
import org.vertx.java.core.Future;
import org.vertx.java.core.spi.cluster.ClusterManager;
import org.vertx.java.platform.Verticle;
import org.vertx.java.platform.impl.DefaultPlatformManager;

import java.util.logging.Logger;

/**
 * Created by buremba <Burak Emre Kabakcı> on 20/09/14 13:26.
 */
public class MemberShipListener extends Verticle {
    private HazelcastInstanceProxy hazelcast;
    public static Logger logging = Logger.getLogger(MemberShipListener.class.getName());
    public static int server_id;
    private DistributedCacheAdapter cacheAdapter = ServiceStarter.injector.getInstance(DistributedCacheAdapter.class);
    private DatabaseAdapter databaseAdapter = ServiceStarter.injector.getInstance(DatabaseAdapter.class);

    public static int getServerId() {
        return server_id;
    }

    @Override
    public void start(Future<Void> startedResult) {
        super.start(startedResult);

        try {
            final DefaultPlatformManager mgr = (DefaultPlatformManager) FieldUtils.readField(container, "mgr", true);
            final ClusterManager cluster = (ClusterManager) FieldUtils.readField(mgr, "clusterManager", true);
            this.hazelcast = (HazelcastInstanceProxy) FieldUtils.readField(cluster, "hazelcast", true);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

        server_id = (int) hazelcast.getAtomicLong("server_id").incrementAndGet();
        hazelcast.getCluster().addMembershipListener(new MembershipListener() {
            @Override
            public void memberAdded(MembershipEvent membershipEvent) {
                String nodeID = membershipEvent.getMember().getUuid();
                logging.finest("say welcome to +" + nodeID + "!");
            }

            @Override
            public void memberRemoved(MembershipEvent membershipEvent) {
                String nodeID = membershipEvent.getMember().getUuid();
                logging.fine("it seems " + nodeID + " is down. checking last check-in timestamp.");
                long downTimestamp = cacheAdapter.getCounter(nodeID);
                logging.fine("node " + nodeID + " is down until " + downTimestamp);

//                databaseAdapter.batch("", (int) downTimestamp, server_id);
            }

            @Override
            public void memberAttributeChanged(MemberAttributeEvent memberAttributeEvent) {

            }
        });

        startedResult.complete();
    }

    @Override
    public void stop() {
        super.stop();
    }
}
