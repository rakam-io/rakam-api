package org.rakam.ui.report;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import org.rakam.ui.DashboardService.DashboardItem;
import org.rakam.ui.customreport.CustomReport;
import org.rakam.ui.page.CustomPageDatabase.Page;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class UIRecipe {
    private final List<Report> reports;
    private final List<CustomReport> customReports;
    private final List<Page> customPages;
    private final List<DashboardBuilder> dashboards;

    @JsonCreator
    public UIRecipe(@JsonProperty("custom_reports") List<CustomReport> customReports,
                    @JsonProperty("custom_pages") List<Page> customPages,
                    @JsonProperty("dashboards") List<DashboardBuilder> dashboards,
                    @JsonProperty("reports") List<Report> reports) {
        this.customReports = customReports == null ? ImmutableList.of() : customReports;
        this.customPages = customPages == null ? ImmutableList.of() : customPages;
        this.reports = reports == null ? ImmutableList.of() : ImmutableList.copyOf(reports);
        this.dashboards = dashboards == null ? ImmutableList.of() : ImmutableList.copyOf(dashboards);
    }

    @JsonProperty("custom_pages")
    public List<Page> getCustomPages() {
        return customPages;
    }

    @JsonProperty("custom_reports")
    public List<CustomReport> getCustomReports() {
        return customReports;
    }

    @JsonProperty("dashboards")
    public List<DashboardBuilder> getDashboards() {
        return dashboards;
    }

    @JsonProperty("reports")
    public List<Report> getReports() {
        return reports;
    }

    public static class DashboardBuilder {
        public final String name;
        public final List<DashboardItem> items;
        public final Map<String, Object> options;
        public final Duration refreshDuration;

        @JsonCreator
        public DashboardBuilder(
                @JsonProperty("name") String name,
                @JsonProperty("items") List<DashboardItem> items,
                @JsonProperty("options") Map<String, Object> options,
                @JsonProperty("refreshDuration") Duration refreshDuration) {
            this.name = name;
            this.items = items;
            this.options = options;
            this.refreshDuration = refreshDuration;
        }
    }
}
