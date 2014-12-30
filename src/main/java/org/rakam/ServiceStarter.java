package org.rakam;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.hazelcast.config.FileSystemXmlConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import org.rakam.analysis.AnalysisRuleMap;
import org.rakam.analysis.rule.aggregation.TimeSeriesAggregationRule;
import org.rakam.cluster.ClusterMemberManager;
import org.rakam.database.AnalysisRuleDatabase;
import org.rakam.server.WebServer;
import org.rakam.server.http.HTTPServer;
import org.rakam.util.TimeUtil;

import java.io.FileNotFoundException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by buremba on 21/12/13.
 */

public class ServiceStarter {
    int cpuCore = Runtime.getRuntime().availableProcessors();

    public static void main(String[] args) throws FileNotFoundException, InterruptedException {
        Injector injector = Guice.createInjector(new ServiceRecipe());

        FileSystemXmlConfig config;
        try {
            config = new FileSystemXmlConfig("config/cluster.xml");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return;
        }

        config.setInstanceName("analytics");
        HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance(config);
        hazelcastInstance.getCluster().addMembershipListener(new MembershipListener() {
            @Override
            public void memberAdded(MembershipEvent membershipEvent) {
                System.out.println("naber");
            }

            @Override
            public void memberRemoved(MembershipEvent membershipEvent) {

            }

            @Override
            public void memberAttributeChanged(MemberAttributeEvent memberAttributeEvent) {

            }
        });
        Thread.sleep(4000);

        System.out.println("tamam");
        FileSystemXmlConfig configa;
        try {
            configa = new FileSystemXmlConfig("config/cluster.xml");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return;
        }
        HazelcastInstance h0 = Hazelcast.newHazelcastInstance(configa);

        long memberId = hazelcastInstance.getIdGenerator("memberId").newId();
        ClusterMemberManager listener = new ClusterMemberManager((int) memberId);
        hazelcastInstance.getCluster().addMembershipListener(listener);

        final AnalysisRuleDatabase ruleDatabase = injector.getInstance(AnalysisRuleDatabase.class);
        final AnalysisRuleMap analysisRuleMap = new AnalysisRuleMap(ruleDatabase.getAllRules());

        final int now = TimeUtil.UTCTime();
        final int nextHour = (now / TimeUtil.HOUR) * TimeUtil.HOUR;
        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                analysisRuleMap.values().forEach(analysisRules -> analysisRules.forEach(analysisRule -> {
                    if (analysisRule instanceof TimeSeriesAggregationRule) {
//                        System.out.println(hz.getMap(analysisRule.id()).localKeySet());

//                        DB db = DBMaker.newFileDB(new File("deneme/"+md.digest(bytesOfMessage))))
//                                .closeOnJvmShutdown()
//                                .make();
                    }
                }));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, nextHour - now, TimeUtil.HOUR, TimeUnit.SECONDS);


        ExecutorService executor = Executors.newCachedThreadPool();
        try {
            new HTTPServer(8888, new WebServer(injector, analysisRuleMap, executor).routeMatcher).run();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }


}


