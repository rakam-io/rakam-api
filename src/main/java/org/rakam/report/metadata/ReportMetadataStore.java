package org.rakam.report.metadata;

import org.rakam.analysis.MaterializedView;
import org.rakam.analysis.Report;
import org.rakam.analysis.TableStrategy;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Created by buremba <Burak Emre Kabakcı> on 21/07/14 05:21.
 */
public interface ReportMetadataStore {
    void saveReport(Report report);

    void createMaterializedView(MaterializedView report);

    void deleteReport(String project, String name);

    Report getReport(String project, String name);

    List<Report> getReports(String project);

    Map<String, List<MaterializedView>> getAllMaterializedViews(TableStrategy strategy);

    void updateMaterializedView(String project, String viewName, Instant lastUpdate);
}
