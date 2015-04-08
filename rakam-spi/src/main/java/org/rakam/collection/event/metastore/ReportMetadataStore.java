package org.rakam.collection.event.metastore;

import org.rakam.plugin.ContinuousQuery;
import org.rakam.plugin.Report;

import java.util.List;
import java.util.Map;

/**
 * Created by buremba <Burak Emre Kabakcı> on 21/07/14 05:21.
 */
public interface ReportMetadataStore {
    public void saveReport(Report report);

    public void deleteReport(String project, String name);

    public Report getReport(String project, String name);

    public List<Report> getReports(String project);

    public void createContinuousQuery(ContinuousQuery report);

    public void deleteContinuousQuery(String project, String name);

    public List<ContinuousQuery> getContinuousQueries(String project);

    public ContinuousQuery getContinuousQuery(String project, String name);

    Map<String, List<ContinuousQuery>> getAllContinuousQueries();
}
