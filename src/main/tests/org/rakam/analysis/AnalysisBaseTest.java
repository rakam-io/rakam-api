package org.rakam.analysis;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.hazelcast.config.FileSystemXmlConfig;
import com.hazelcast.core.Hazelcast;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.rakam.RakamTestHelper;
import org.rakam.stream.ActorCacheAdapter;
import org.rakam.stream.MetricStreamAdapter;
import org.rakam.stream.TimeSeriesStreamAdapter;
import org.rakam.stream.kume.KumeStreamAdapter;
import org.rakam.stream.local.LocalCache;
import org.rakam.stream.local.LocalCacheImpl;
import org.rakam.collection.event.EventAggregator;
import org.rakam.collection.event.PeriodicCollector;
import org.rakam.database.AnalysisRuleDatabase;
import org.rakam.database.DatabaseAdapter;

import java.io.FileNotFoundException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * Created by buremba <Burak Emre Kabakcı> on 01/11/14 00:49.
 */
public class AnalysisBaseTest extends RakamTestHelper {
    static DatabaseAdapter databaseAdapter = new DummyDatabase();
    static EventAggregator eventAggregator;
    static PeriodicCollector collector;
    static EventAnalyzer eventAnalyzer;
    static AnalysisRuleMap analysisRuleMap;

    protected static String formatTime(int time) {
        return Instant.ofEpochSecond(time).atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ISO_INSTANT);
    }

    @BeforeClass
    public static void start() throws FileNotFoundException {
        Injector injector = Guice.createInjector(new AbstractModule() {
            @Override
            protected void configure() {
                bind(LocalCache.class).to(LocalCacheImpl.class);
                bind(LocalCacheImpl.class).in(Scopes.SINGLETON);

                bind(DatabaseAdapter.class).to(DummyDatabase.class);
                bind(MetricStreamAdapter.class).to(KumeStreamAdapter.class);
                bind(TimeSeriesStreamAdapter.class).to(KumeStreamAdapter.class);
                bind(AnalysisRuleDatabase.class).to(DummyDatabase.class);
                bind(ActorCacheAdapter.class).to(DummyDatabase.class);
            }
        });

        analysisRuleMap = new AnalysisRuleMap();

        eventAggregator = new EventAggregator(injector, analysisRuleMap);
        collector = new PeriodicCollector(injector);
        eventAnalyzer = new EventAnalyzer(injector, analysisRuleMap);
    }

    @Before
    public void clear() {
        analysisRuleMap.clear();

    }

    @AfterClass
    public static void after() throws FileNotFoundException {
        Hazelcast.getHazelcastInstanceByName("analytics").shutdown();
    }
}
