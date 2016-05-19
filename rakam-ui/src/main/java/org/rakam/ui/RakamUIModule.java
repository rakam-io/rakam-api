package org.rakam.ui;

import com.google.common.base.Optional;
import com.google.common.eventbus.Subscribe;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.multibindings.OptionalBinder;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.config.EncryptionConfig;
import org.rakam.config.JDBCConfig;
import org.rakam.plugin.InjectionHook;
import org.rakam.plugin.RakamModule;
import org.rakam.plugin.SystemEvents;
import org.rakam.plugin.SystemEvents.ProjectCreatedEvent;
import org.rakam.server.http.HttpService;
import org.rakam.ui.customreport.CustomPageHttpService;
import org.rakam.ui.customreport.CustomReport;
import org.rakam.ui.customreport.CustomReportHttpService;
import org.rakam.ui.customreport.CustomReportMetadata;
import org.rakam.ui.customreport.JDBCCustomReportMetadata;
import org.rakam.ui.page.CustomPageDatabase;
import org.rakam.ui.page.FileBackedCustomPageDatabase;
import org.rakam.ui.page.JDBCCustomPageDatabase;
import org.rakam.ui.report.Report;
import org.rakam.ui.report.ReportHttpService;
import org.rakam.ui.user.UserUtilHttpService;
import org.rakam.ui.user.WebUserHttpService;
import org.rakam.util.ConditionalModule;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;

import static io.airlift.configuration.ConfigBinder.configBinder;


@ConditionalModule(config = "ui.enable", value = "true")
public class RakamUIModule extends RakamModule {
    @Override
    protected void setup(Binder binder) {
        configBinder(binder).bindConfig(EncryptionConfig.class);

        RakamUIConfig rakamUIConfig = buildConfigObject(RakamUIConfig.class);

        OptionalBinder<CustomPageDatabase> customPageDb = OptionalBinder.newOptionalBinder(binder, CustomPageDatabase.class);
        if(rakamUIConfig.getCustomPageBackend() != null) {
            switch (rakamUIConfig.getCustomPageBackend()) {
                case FILE:
                    customPageDb.setBinding().to(FileBackedCustomPageDatabase.class).in(Scopes.SINGLETON);
                    break;
                case JDBC:
                    customPageDb.setBinding().to(JDBCCustomPageDatabase.class).in(Scopes.SINGLETON);
                    break;
            }
        }

        binder.bind(JDBCPoolDataSource.class)
                .annotatedWith(Names.named("ui.metadata.jdbc"))
                .toInstance(JDBCPoolDataSource.getOrCreateDataSource(buildConfigObject(JDBCConfig.class, "ui.metadata.jdbc")));

        Multibinder<InjectionHook> hooks = Multibinder.newSetBinder(binder, InjectionHook.class);
        hooks.addBinding().to(DatabaseScript.class);

        binder.bind(ProjectDeleteEventListener.class).asEagerSingleton();
        binder.bind(ReportMetadata.class).to(JDBCReportMetadata.class).in(Scopes.SINGLETON);
        binder.bind(CustomReportMetadata.class).to(JDBCCustomReportMetadata.class).in(Scopes.SINGLETON);
        binder.bind(DefaultDashboardCreator.class).asEagerSingleton();

        Multibinder<HttpService> httpServices = Multibinder.newSetBinder(binder, HttpService.class);
        httpServices.addBinding().to(WebUserHttpService.class);
        httpServices.addBinding().to(ReportHttpService.class);
        httpServices.addBinding().to(CustomReportHttpService.class);
        httpServices.addBinding().to(CustomPageHttpService.class);
        httpServices.addBinding().to(DashboardService.class);
        httpServices.addBinding().to(ProxyWebService.class);
        httpServices.addBinding().to(RakamUIWebService.class);
        httpServices.addBinding().to(UserUtilHttpService.class);
        httpServices.addBinding().to(ClusterService.class);
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

        @Inject
        public DefaultDashboardCreator(DashboardService service) {
            this.service = service;
        }

        @Subscribe
        public void onCreateProject(ProjectCreatedEvent event) {
            service.create(event.project, "My dashboard", null);
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
        public void onDeleteProject(SystemEvents.ProjectDeletedEvent event) {
            for (DashboardService.Dashboard dashboard : dashboardService.list(event.project)) {
                dashboardService.delete(event.project, dashboard.name);
            }
            if(customPageDatabase != null) {
                for (CustomPageDatabase.Page page : customPageDatabase.list(event.project)) {
                    customPageDatabase.delete(event.project, page.slug);
                }
            }
            for (Report report : reportMetadata.getReports(null, event.project)) {
                reportMetadata.delete(null, event.project, report.slug);
            }
            for (Map.Entry<String, List<CustomReport>> types : customReportMetadata.list(event.project).entrySet()) {
                for (CustomReport customReport : types.getValue()) {
                    customReportMetadata.delete(types.getKey(), event.project, customReport.name);
                }
            }
        }
    }

    public static class DatabaseScript implements InjectionHook {
        private final DBI dbi;
        private final RakamUIConfig config;

        @Inject
        public DatabaseScript(@Named("ui.metadata.jdbc") JDBCPoolDataSource dataSource, RakamUIConfig config) {
            dbi = new DBI(dataSource);
            this.config = config;
        }

        @Override
        public void call() {
            try (Handle handle = dbi.open()) {

                handle.createStatement("CREATE TABLE IF NOT EXISTS web_user (" +
                        "  id SERIAL PRIMARY KEY,\n" +
                        "  email TEXT NOT NULL UNIQUE,\n" +
                        "  is_activated BOOLEAN DEFAULT false NOT NULL,\n" +
                        "  password TEXT,\n" +
                        "  name TEXT NOT NULL,\n" +
                        "  created_at TIMESTAMP NOT NULL\n" +
                        "  )")
                        .execute();

                handle.createStatement("CREATE TABLE IF NOT EXISTS web_user_project (" +
                        "  user_id INTEGER REFERENCES web_user(id),\n" +
                        "  api_url TEXT,\n" +
                        "  project TEXT NOT NULL,\n" +
                        "  scope_expression TEXT,\n" +
                        "  has_read_permission BOOLEAN NOT NULL,\n" +
                        "  has_write_permission BOOLEAN NOT NULL,\n" +
                        "  is_admin BOOLEAN DEFAULT false NOT NULL\n" +
                        "  )")
                        .execute();

                handle.createStatement("CREATE TABLE IF NOT EXISTS reports (" +
                        "  project VARCHAR(255) NOT NULL," +
                        "  user_id INT REFERENCES web_user(id)," +
                        "  slug VARCHAR(255) NOT NULL," +
                        "  category VARCHAR(255)," +
                        "  name VARCHAR(255) NOT NULL," +
                        "  query TEXT NOT NULL," +
                        "  options TEXT," +
                        "  shared BOOLEAN NOT NULL DEFAULT false," +
                        "  created_at TIMESTAMP NOT NULL DEFAULT now()," +
                        "  CONSTRAINT address UNIQUE(project, user_id, slug)" +
                        "  )")
                        .execute();

                handle.createStatement("CREATE TABLE IF NOT EXISTS custom_reports (" +
                        "  report_type VARCHAR(255) NOT NULL," +
                        "  user_id INT REFERENCES web_user(id)," +
                        "  project VARCHAR(255) NOT NULL," +
                        "  name VARCHAR(255) NOT NULL," +
                        "  data TEXT NOT NULL," +
                        "  PRIMARY KEY (report_type, project, name)" +
                        "  )")
                        .execute();

                handle.createStatement("CREATE TABLE IF NOT EXISTS rakam_cluster (" +
                        "  user_id INT REFERENCES web_user(id)," +
                        "  api_url VARCHAR(255) NOT NULL," +
                        "  lock_key VARCHAR(255)," +
                        "  PRIMARY KEY (user_id, api_url)" +
                        "  )")
                        .execute();

                handle.createStatement("CREATE TABLE IF NOT EXISTS dashboard (" +
                        "  id SERIAL," +
                        "  project VARCHAR(255) NOT NULL," +
                        "  user_id INT REFERENCES web_user(id)," +
                        "  name VARCHAR(255) NOT NULL," +
                        "  options TEXT," +
                        "  UNIQUE (project, name)," +
                        "  PRIMARY KEY (id)" +
                        "  )")
                        .execute();
                handle.createStatement("CREATE TABLE IF NOT EXISTS dashboard_items (" +
                        "  id SERIAL," +
                        "  dashboard int NOT NULL REFERENCES dashboard(id) ON DELETE CASCADE," +
                        "  name VARCHAR(255) NOT NULL," +
                        "  directive VARCHAR(255) NOT NULL," +
                        "  data TEXT NOT NULL," +
                        "  PRIMARY KEY (id)" +
                        "  )")
                        .execute();

                if(config.getCustomPageBackend() == CustomPageBackend.JDBC) {
                    handle.createStatement("CREATE TABLE IF NOT EXISTS custom_page (" +
                            "  project VARCHAR(255) NOT NULL," +
                            "  name VARCHAR(255) NOT NULL," +
                            "  user_id INT REFERENCES web_user(id)," +
                            "  slug VARCHAR(255) NOT NULL," +
                            "  category VARCHAR(255)," +
                            "  data TEXT NOT NULL," +
                            "  PRIMARY KEY (project, slug)" +
                            "  )")
                            .execute();
                }
            }
        }
    }
}
