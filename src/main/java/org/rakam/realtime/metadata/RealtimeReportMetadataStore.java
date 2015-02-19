package org.rakam.realtime.metadata;

import org.rakam.realtime.RealTimeReport;

import java.util.List;

/**
 * Created by buremba <Burak Emre Kabakcı> on 19/02/15 04:00.
 */
public interface RealtimeReportMetadataStore {
    void saveReport(RealTimeReport report);

    void deleteReport(String project, String name);

    RealTimeReport getReport(String project, String name);

    List<RealTimeReport> getReports(String project);
}
