package org.rakam.ui.report;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.rakam.ui.DashboardService;
import org.rakam.ui.ReportMetadata;
import org.rakam.ui.customreport.CustomReport;
import org.rakam.ui.customreport.CustomReportMetadata;
import org.rakam.ui.page.CustomPageDatabase;
import org.rakam.util.AlreadyExistsException;
import org.rakam.util.RakamException;

import javax.inject.Inject;
import java.util.List;
import java.util.stream.Collectors;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;

public class UIRecipeHandler {
    private final Optional<ReportMetadata> reportMetadata;
    private final Optional<CustomReportMetadata> customReportMetadata;
    private final Optional<CustomPageDatabase> customPageDatabase;
    private final Optional<DashboardService> dashboardService;

    @Inject
    public UIRecipeHandler(
                         Optional<CustomReportMetadata> customReportMetadata,
                         Optional<CustomPageDatabase> customPageDatabase,
                         Optional<DashboardService> dashboardService,
                         Optional<ReportMetadata> reportMetadata) {
        this.customReportMetadata = customReportMetadata;
        this.customPageDatabase = customPageDatabase;
        this.reportMetadata = reportMetadata;
        this.dashboardService = dashboardService;
    }

    public UIRecipe export(int project) {
        final List<Report> reports;
        if(reportMetadata.isPresent()) {
            reports = reportMetadata.get()
                    .getReports(null, project).stream()
                    .map(r -> new Report(r.slug, r.name, r.category, r.query, r.options, r.shared))
                    .collect(Collectors.toList());
        } else {
            reports = ImmutableList.of();
        }

        final List<CustomReport> customReports;
        if(customReportMetadata.isPresent()) {
            customReports = customReportMetadata.get().list(project).entrySet().stream().flatMap(a -> a.getValue().stream())
                    .map(r -> new CustomReport(r.reportType, r.name, r.data))
                    .collect(Collectors.toList());
        } else {
            customReports = ImmutableList.of();
        }

        final List<CustomPageDatabase.Page> customPages;
        if(customPageDatabase.isPresent()) {
            customPages = customPageDatabase.get()
                    .list(project).stream()
                    .map(r -> new CustomPageDatabase.Page(r.name, r.slug, r.category, customPageDatabase.get().get(project, r.slug)))
                    .collect(Collectors.toList());
        } else {
            customPages = ImmutableList.of();
        }

        List<UIRecipe.DashboardBuilder> dashboards;
        if(dashboardService.isPresent()) {
            dashboards = dashboardService.get().list(project).stream()
                    .map(a -> new UIRecipe.DashboardBuilder(a.name, dashboardService.get().get(project, a.name)))
                    .collect(Collectors.toList());
        } else {
            dashboards = ImmutableList.of();
        }

        return new UIRecipe(customReports, customPages, dashboards, reports);
    }

    public void install(UIRecipe recipe, int project, boolean overrideExisting) {
        installInternal(recipe, project, overrideExisting);
    }

    public void installInternal(UIRecipe recipe, int project, boolean overrideExisting) {
        recipe.getReports().stream()
                .forEach(report -> {
                    try {
                        reportMetadata.get().save(null, project, report);
                    } catch (AlreadyExistsException e) {
                        if (overrideExisting) {
                            reportMetadata.get().update(null, project, report);
                        } else {
                            throw Throwables.propagate(e);
                        }
                    }
                });

        recipe.getDashboards().stream()
                .forEach(report -> {
                    int dashboard;
                    try {
                        dashboard = dashboardService.get().create(project, report.name, ImmutableMap.of()).id;
                    } catch (AlreadyExistsException e) {
                        dashboard = dashboardService.get().list(project).stream().filter(a -> a.name.equals(report.name)).findAny().get().id;
                        dashboardService.get().delete(project, dashboard);
                        dashboard = dashboardService.get().create(project, report.name, ImmutableMap.of()).id;
                    }

                    for (DashboardService.DashboardItem item : report.items) {
                        dashboardService.get().addToDashboard(project, dashboard, item.name, item.directive, item.data);
                    }
                });

        recipe.getCustomReports().stream()
                .forEach(customReport -> {
                    try {
                        customReportMetadata.get().save(null, project, customReport);
                    } catch (AlreadyExistsException e) {
                        if (overrideExisting) {
                            customReportMetadata.get().update(project, customReport);
                        } else {
                            throw Throwables.propagate(e);
                        }
                    }
                });

        if(customPageDatabase.isPresent()) {
            recipe.getCustomPages().stream()
                    .forEach(customReport -> {
                        try {
                            customPageDatabase.get().save(null, project, customReport);
                        } catch (AlreadyExistsException e) {
                            if (overrideExisting) {
                                customPageDatabase.get().delete(project, customReport.slug);
                                customPageDatabase.get().save(null, project, customReport);
                            } else {
                                throw Throwables.propagate(e);
                            }
                        }
                    });
        } else
        if(recipe.getCustomPages().size() > 0) {
            throw new RakamException("Custom page feature is not supported", BAD_REQUEST);
        }
    }
}
