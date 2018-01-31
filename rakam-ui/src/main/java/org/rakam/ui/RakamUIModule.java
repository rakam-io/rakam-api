package org.rakam.ui;

import com.google.auto.service.AutoService;
import com.google.common.base.Optional;
import com.google.common.eventbus.Subscribe;
import com.google.inject.Binder;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.multibindings.OptionalBinder;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.FlywayException;
import org.rakam.analysis.CustomParameter;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.config.EncryptionConfig;
import org.rakam.config.JDBCConfig;
import org.rakam.plugin.RakamModule;
import org.rakam.plugin.stream.EventStreamConfig;
import org.rakam.plugin.user.UserPluginConfig;
import org.rakam.report.EmailClientConfig;
import org.rakam.report.eventexplorer.EventExplorerConfig;
import org.rakam.report.realtime.RealTimeConfig;
import org.rakam.server.http.HttpRequestHandler;
import org.rakam.server.http.HttpService;
import org.rakam.ui.UIEvents.ProjectCreatedEvent;
import org.rakam.ui.UIPermissionParameterProvider.Project;
import org.rakam.ui.customreport.*;
import org.rakam.ui.page.CustomPageDatabase;
import org.rakam.ui.page.FileBackedCustomPageDatabase;
import org.rakam.ui.page.JDBCCustomPageDatabase;
import org.rakam.ui.report.Report;
import org.rakam.ui.report.ReportHttpService;
import org.rakam.ui.report.UIRecipeHandler;
import org.rakam.ui.report.UIRecipeHttpService;
import org.rakam.ui.user.UserSubscriptionHttpService;
import org.rakam.ui.user.WebUserHttpService;
import org.rakam.ui.user.WebUserService;
import org.rakam.util.NotFoundHandler;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.util.IntegerMapper;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;

import static io.airlift.configuration.ConfigBinder.configBinder;

@AutoService(RakamModule.class)
public class RakamUIModule
        extends RakamModule {
    @Override
    protected void setup(Binder binder) {
        RakamUIConfig rakamUIConfig = buildConfigObject(RakamUIConfig.class);
        OptionalBinder.newOptionalBinder(binder, WebUserService.class);

        if (!rakamUIConfig.getEnableUI()) {
            return;
        }

        binder.bind(WebUserService.class).asEagerSingleton();
        binder.bind(DashboardService.class).asEagerSingleton();
        binder.bind(UserDefaultService.class).asEagerSingleton();

        configBinder(binder).bindConfig(EmailClientConfig.class);
        configBinder(binder).bindConfig(EncryptionConfig.class);
        configBinder(binder).bindConfig(UserPluginConfig.class);
        configBinder(binder).bindConfig(RealTimeConfig.class);
        configBinder(binder).bindConfig(EventStreamConfig.class);
        configBinder(binder).bindConfig(EventExplorerConfig.class);
        configBinder(binder).bindConfig(UserPluginConfig.class);

        OptionalBinder.newOptionalBinder(binder, AuthService.class);
        OptionalBinder.newOptionalBinder(binder, CustomPageDatabase.class);
        OptionalBinder.newOptionalBinder(binder, CustomReportMetadata.class);

        if (rakamUIConfig.getCustomPageBackend() != null) {
            switch (rakamUIConfig.getCustomPageBackend()) {
                case FILE:
                    binder.bind(FileBackedCustomPageDatabase.class).in(Scopes.SINGLETON);
                    break;
                case JDBC:
                    binder.bind(JDBCCustomPageDatabase.class).in(Scopes.SINGLETON);
                    break;
            }
        }

        Multibinder<CustomParameter> customParameters = Multibinder.newSetBinder(binder, CustomParameter.class);
        customParameters.addBinding().toProvider(UIPermissionParameterProvider.class);

        JDBCPoolDataSource pool = JDBCPoolDataSource.getOrCreateDataSource(buildConfigObject(JDBCConfig.class, "ui.metadata.jdbc"));
        binder.bind(JDBCPoolDataSource.class)
                .annotatedWith(Names.named("ui.metadata.jdbc"))
                .toInstance(pool);

        binder.bind(FlywayExecutor.class).asEagerSingleton();

        binder.bind(ProjectDeleteEventListener.class).asEagerSingleton();
        binder.bind(ReportMetadata.class).to(JDBCReportMetadata.class).in(Scopes.SINGLETON);
        binder.bind(CustomReportMetadata.class).to(JDBCCustomReportMetadata.class).in(Scopes.SINGLETON);
        binder.bind(DefaultDashboardCreator.class).asEagerSingleton();

        binder.bind(WebUserHttpService.class).asEagerSingleton();

        Multibinder<HttpService> httpServices = Multibinder.newSetBinder(binder, HttpService.class);
        httpServices.addBinding().to(WebUserHttpService.class);
        httpServices.addBinding().to(ReportHttpService.class);
        httpServices.addBinding().to(DashboardService.class);
        httpServices.addBinding().to(CustomReportHttpService.class);
        httpServices.addBinding().to(CustomPageHttpService.class);
        httpServices.addBinding().to(ProxyWebService.class);
        httpServices.addBinding().to(WebHookUIHttpService.class);
        httpServices.addBinding().to(ScheduledTaskUIHttpService.class);
        httpServices.addBinding().to(CustomEventMapperUIHttpService.class);
        httpServices.addBinding().to(ClusterService.class);
        httpServices.addBinding().to(UIRecipeHttpService.class);
        binder.bind(UIRecipeHandler.class).asEagerSingleton();

        if (rakamUIConfig.getScreenCaptureService() != null) {
            httpServices.addBinding().to(ScheduledEmailService.class);
        }
        if (rakamUIConfig.getStripeKey() != null) {
            httpServices.addBinding().to(UserSubscriptionHttpService.class);
        }
        httpServices.addBinding().to(RakamUIWebService.class);

        binder.bind(HttpRequestHandler.class)
                .annotatedWith(NotFoundHandler.class)
                .toProvider(WebsiteRequestHandler.class);
    }

    @Override
    public String name() {
        return "Web Interface for Rakam APIs";
    }

    @Override
    public String description() {
        return "Can be used as a BI tool and a tool that allows you to create your customized analytics service frontend.";
    }

    public enum CustomPageBackend {
        FILE, JDBC
    }

    public static class DefaultDashboardCreator {

        private final DashboardService service;
        private final DBI dbi;

        @Inject
        public DefaultDashboardCreator(DashboardService service, @javax.inject.Named("ui.metadata.jdbc") JDBCPoolDataSource dataSource) {
            this.service = service;
            this.dbi = new DBI(dataSource);
        }

        @Subscribe
        public void onCreateProject(ProjectCreatedEvent event) {
            Integer id;
            try (Handle handle = dbi.open()) {
                id = handle.createQuery("select user_id from web_user_project where id = :id")
                        .bind("id", event.project).map(IntegerMapper.FIRST).first();
            }
            Project project = new Project(event.project, id);
            DashboardService.Dashboard dashboard = service.create(project, "My dashboard", true, null, null);
            service.setDefault(project, dashboard.id);
        }
    }

    public static class ProjectDeleteEventListener {
        private final DashboardService dashboardService;
        private final CustomPageDatabase customPageDatabase;
        private final ReportMetadata reportMetadata;
        private final CustomReportMetadata customReportMetadata;

        @Inject
        public ProjectDeleteEventListener(DashboardService dashboardService,
                                          Optional<CustomPageDatabase> customPageDatabase,
                                          ReportMetadata reportMetadata,
                                          CustomReportMetadata customReportMetadata) {
            this.reportMetadata = reportMetadata;
            this.customReportMetadata = customReportMetadata;
            this.customPageDatabase = customPageDatabase.orNull();
            this.dashboardService = dashboardService;
        }

        @Subscribe
        public void onDeleteProject(UIEvents.ProjectDeletedEvent event) {
            for (DashboardService.Dashboard dashboard : dashboardService.list(new Project(0, event.project)).dashboards) {
                dashboardService.delete(new Project(event.project, 0), dashboard.id);
            }
            if (customPageDatabase != null) {
                for (CustomPageDatabase.Page page : customPageDatabase.list(event.project)) {
                    customPageDatabase.delete(event.project, page.slug);
                }
            }
            for (Report report : reportMetadata.list(null, event.project)) {
                reportMetadata.delete(null, event.project, report.slug);
            }
            for (Map.Entry<String, List<CustomReport>> types : customReportMetadata.list(event.project).entrySet()) {
                for (CustomReport customReport : types.getValue()) {
                    customReportMetadata.delete(types.getKey(), event.project, customReport.name);
                }
            }
        }
    }

    public static class FlywayExecutor {
        @Inject
        public FlywayExecutor(@Named("ui.metadata.jdbc") JDBCConfig config) {
            Flyway flyway = new Flyway();
            flyway.setBaselineOnMigrate(true);
            flyway.setLocations("db/migration/ui");
            flyway.setTable("schema_version_ui");
            flyway.setDataSource(config.getUrl(), config.getUsername(), config.getPassword());
            try {
                flyway.migrate();
            } catch (FlywayException e) {
                flyway.repair();
            }
        }
    }

    public static class WebsiteRequestHandler
            implements Provider<HttpRequestHandler> {

        private final RakamUIWebService service;

        @Inject
        public WebsiteRequestHandler(RakamUIConfig config) {

            service = new RakamUIWebService(config);
        }

        @Override
        public HttpRequestHandler get() {
            return service::main;
        }
    }
}
