package org.rakam.report.metadata;

import org.rakam.analysis.ContinuousQuery;
import org.rakam.analysis.Report;
import org.rakam.analysis.TableStrategy;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Created by buremba <Burak Emre Kabakcı> on 21/07/14 05:21.
 */
public interface ReportMetadataStore {
    public void saveReport(Report report);

    public void createContinuousQuery(ContinuousQuery report);

    public void deleteReport(String project, String name);

    public Report getReport(String project, String name);

    public List<Report> getReports(String project);

    Map<String, List<ContinuousQuery>> getAllContinuousQueries(TableStrategy strategy);

    void updateContinuousQuery(String project, String viewName, Instant lastUpdate);
}
