package org.rakam.analysis.realtime;

import org.rakam.report.QueryError;
import org.rakam.report.realtime.RealTimeConfig;
import org.rakam.report.realtime.RealTimeReport;

import javax.inject.Inject;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class RealtimeService {
    private final RealtimeMetadataService metadataService;
    private final RealtimeEventProcessor processor;
    private final RealTimeConfig config;

    @Inject
    public RealtimeService(RealtimeMetadataService metadataService, RealTimeConfig config, RealtimeEventProcessor processor) {
        this.metadataService = metadataService;
        this.processor = processor;
        this.config = config;
    }

    public CompletableFuture<QueryError> create(String project, RealTimeReport report) {
        metadataService.save(project, report);
        return CompletableFuture.completedFuture(null);
    }

    public CompletableFuture<QueryError> delete(String project, String tableName) {
        metadataService.delete(project, tableName);
        return CompletableFuture.completedFuture(null);
    }

    public CompletableFuture<RealTimeQueryResult> query(String project,
                                                        String tableName,
                                                        String filter,
                                                        RealTimeReport.Measure measure,
                                                        List<String> dimensions,
                                                        Boolean aggregate,
                                                        Instant dateStart,
                                                        Instant dateEnd) {
        if (dateEnd == null) {
            long amountToSubtract = config.getSlideInterval().toMillis();
            dateEnd = Instant.ofEpochMilli((Instant.now().toEpochMilli() / amountToSubtract)
                    * amountToSubtract).minus(amountToSubtract, ChronoUnit.MILLIS);
        }

        if (dateStart == null) {
            dateStart = dateEnd.minus(config.getWindowInterval().toMillis(), ChronoUnit.MILLIS);
        }

        return processor.query(project, tableName, filter, measure, dimensions, aggregate,
                dateStart, dateEnd);
    }

    public List<RealTimeReport> list(String project) {
        return metadataService.list(project);
    }

    public static class RealTimeQueryResult {
        public final long start;
        public final long end;
        public final long slideInterval;
        public final Object result;

        public RealTimeQueryResult(long start, long end, long slideInterval, Object result) {
            this.start = start;
            this.end = end;
            this.slideInterval = slideInterval;
            this.result = result;
        }
    }
}
