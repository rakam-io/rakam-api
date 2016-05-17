package org.rakam.ui.customreport;

import java.util.List;
import java.util.Map;

public interface CustomReportMetadata {
    void save(Integer user, String project, CustomReport report);
    CustomReport get(String reportType, String project, String name);
    List<CustomReport> list(String reportType, String project);
    Map<String, List<CustomReport>> list(String project);
    void delete(String reportType, String project, String name);
    void update(String project, CustomReport report);
    List<String> types(String project);
}
