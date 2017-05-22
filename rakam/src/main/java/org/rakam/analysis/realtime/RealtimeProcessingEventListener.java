package org.rakam.analysis.realtime;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.collect.Table;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.cookie.Cookie;
import org.apache.avro.generic.GenericRecord;
import org.rakam.collection.Event;
import org.rakam.plugin.EventMapper;
import org.rakam.report.realtime.RealTimeReport;

import javax.annotation.Nonnull;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static java.util.concurrent.TimeUnit.MINUTES;

public class RealtimeProcessingEventListener implements EventMapper
{
//    private final Table<String, String, String> map;
    private final Map<String, List<RealTimeReport>> reports;

    public RealtimeProcessingEventListener()
    {
        LoadingCache<RealTimeReport, String> graphs = Caffeine.newBuilder()
                .expireAfterAccess(5, MINUTES)
                .expireAfterWrite(10, MINUTES)
                .expireAfter(new Expiry<RealTimeReport, String>()
                {
                    public long expireAfterCreate(RealTimeReport key, String graph, long currentTime)
                    {
//                        long seconds = graph.creationDate().plusHours(5)
//                                .minus(System.currentTimeMillis(), MILLIS)
//                                .toEpochSecond();
                        long seconds = 0;
                        return TimeUnit.SECONDS.toNanos(seconds);
                    }

                    public long expireAfterUpdate(RealTimeReport key, String graph,
                            long currentTime, long currentDuration)
                    {
                        return currentDuration;
                    }

                    public long expireAfterRead(RealTimeReport key, String graph,
                            long currentTime, long currentDuration)
                    {
                        return currentDuration;
                    }
                })
                .build(k1 -> createExpensiveGraph(k1));
        this.reports = new HashMap<>();
    }

    private String createExpensiveGraph(RealTimeReport key) {
        return "";
    }

    @Override
    public CompletableFuture<List<Cookie>> mapAsync(Event event, RequestParams requestParams, InetAddress sourceAddress, HttpHeaders responseHeaders)
    {
        List<RealTimeReport> realTimeReports = reports.get(event.project());
        if(realTimeReports == null) {
            return COMPLETED_EMPTY_FUTURE;
        }

        for (RealTimeReport report : realTimeReports) {
            if(report.collections.isEmpty() && !report.collections.contains(event.collection())) {
                continue;
            }

            Predicate<GenericRecord> filter = null;
            String filter1 = report.filter;
        }

        return null;
    }
}
