package org.rakam;

import com.google.inject.Guice;
import com.google.inject.Injector;
import org.rakam.analysis.AnalysisRuleMap;
import org.rakam.database.AnalysisRuleDatabase;
import org.rakam.kume.Cluster;
import org.rakam.kume.ClusterBuilder;
import org.rakam.server.WebServer;
import org.rakam.server.http.HTTPServer;

import java.io.FileNotFoundException;

/**
 * Created by buremba on 21/12/13.
 */

public class ServiceStarter {

    public static void main(String[] args) throws FileNotFoundException, InterruptedException {

        Cluster cluster = new ClusterBuilder().start();

        Injector injector = Guice.createInjector(new ServiceRecipe(cluster));

        final AnalysisRuleDatabase ruleDatabase = injector.getInstance(AnalysisRuleDatabase.class);
        final AnalysisRuleMap analysisRuleMap = new AnalysisRuleMap(ruleDatabase.getAllRules());

//        final int now = TimeUtil.UTCTime();
//        final int nextHour = (now / TimeUtil.HOUR) * TimeUtil.HOUR;
//        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
//        scheduledExecutorService.scheduleAtFixedRate(() -> {
//            try {
//                analysisRuleMap.values().forEach(analysisRules -> analysisRules.forEach(analysisRule -> {
//                    if (analysisRule instanceof TimeSeriesAggregationRule) {
//                        System.out.println(hz.getMap(analysisRule.id()).localKeySet());

//                        DB db = DBMaker.newFileDB(new File("deneme/"+md.digest(bytesOfMessage))))
//                                .closeOnJvmShutdown()
//                                .make();
//                    }
//                }));
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        }, nextHour - now, TimeUtil.HOUR, TimeUnit.SECONDS);


        try {
            new HTTPServer(8888, new WebServer(injector, analysisRuleMap).routeMatcher).run();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }


}


