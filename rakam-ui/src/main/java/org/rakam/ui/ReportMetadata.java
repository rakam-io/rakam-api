package org.rakam.ui;

import org.rakam.ui.report.Report;

import java.util.List;

public interface ReportMetadata {
    List<Report> list(Integer requestedUserId, int project);

    void delete(Integer userId, int project, String slug);

    void save(Integer userId, int project, Report report);

    Report get(Integer requestedUserId, int project, String slug);

    Report update(Integer userId, int project, Report report);
}
