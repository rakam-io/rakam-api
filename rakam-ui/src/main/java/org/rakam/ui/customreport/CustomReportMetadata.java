package org.rakam.ui.customreport;

import java.util.List;
import java.util.Map;

public interface CustomReportMetadata {
    void save(Integer user, int project, CustomReport report);

    CustomReport get(String reportType, int project, String name);

    List<CustomReport> list(String reportType, int project);

    Map<String, List<CustomReport>> list(int project);

    void delete(String reportType, int project, String name);

    void update(int project, CustomReport report);

    List<String> types(int project);
}
