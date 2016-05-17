package org.rakam.ui;

import org.rakam.ui.report.Report;

import java.util.List;

public interface ReportMetadata {
    List<Report> getReports(Integer requestedUserId, String project);
    void delete(Integer userId, String project, String slug);
    void save(Integer userId, String project, Report report);
    Report get(Integer requestedUserId, Integer userId, String project, String slug);
    Report update(Integer userId, String project, Report report);
}
