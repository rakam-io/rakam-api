package org.rakam.report.metadata;

import org.rakam.analysis.Report;

import java.util.List;

/**
 * Created by buremba <Burak Emre Kabakcı> on 21/07/14 05:21.
 */
public interface ReportMetadataStore {
    void saveReport(Report report);

    void deleteReport(String project, String name);

    Report getReport(String project, String name);

    List<Report> getReports(String project);
}
