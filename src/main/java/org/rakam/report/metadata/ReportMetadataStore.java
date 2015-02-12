package org.rakam.report.metadata;

import java.util.Map;

/**
 * Created by buremba <Burak Emre Kabakcı> on 21/07/14 05:21.
 */
public interface ReportMetadataStore {
    void createReport(String project, String name, String query);

    void deleteReport(String project, String name);

    String getReport(String project, String name);

    Map<String, String> getReports(String project);


}
