package org.rakam.ui;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Optional;
import com.google.common.eventbus.Subscribe;
import com.google.inject.Binder;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.multibindings.OptionalBinder;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.analysis.RequestPreProcessorItem;
import org.rakam.config.EncryptionConfig;
import org.rakam.config.JDBCConfig;
import org.rakam.plugin.InjectionHook;
import org.rakam.plugin.RakamModule;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.server.http.RequestPreprocessor;
import org.rakam.ui.UIEvents.ProjectCreatedEvent;
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
import org.rakam.ui.report.UIRecipeHttpService;
import org.rakam.ui.user.UserUtilHttpService;
import org.rakam.ui.user.WebUserHttpService;
import org.rakam.util.ConditionalModule;
import org.rakam.util.RakamException;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.util.IntegerMapper;

import javax.inject.Inject;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.List;
import java.util.Map;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static org.rakam.ui.user.WebUserHttpService.extractUserFromCookie;


@ConditionalModule(config = "ui.enable", value = "true")
public class RakamUIModule extends RakamModule {
    @Override
    protected void setup(Binder binder) {
        configBinder(binder).bindConfig(EncryptionConfig.class);

        RakamUIConfig rakamUIConfig = buildConfigObject(RakamUIConfig.class);

        OptionalBinder<CustomPageDatabase> customPageDb = OptionalBinder.newOptionalBinder(binder, CustomPageDatabase.class);
        if (rakamUIConfig.getCustomPageBackend() != null) {
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

        Multibinder<RequestPreProcessorItem> multibinder = Multibinder.newSetBinder(binder, RequestPreProcessorItem.class);
        multibinder.addBinding().toProvider(UIPermissionCheckProcessorProvider.class);

        binder.bind(ProjectDeleteEventListener.class).asEagerSingleton();
        binder.bind(ReportMetadata.class).to(JDBCReportMetadata.class).in(Scopes.SINGLETON);
        binder.bind(CustomReportMetadata.class).to(JDBCCustomReportMetadata.class).in(Scopes.SINGLETON);
        binder.bind(DefaultDashboardCreator.class).asEagerSingleton();

        Multibinder<HttpService> httpServices = Multibinder.newSetBinder(binder, HttpService.class);
        httpServices.addBinding().to(WebUserHttpService.class);
        httpServices.addBinding().to(ReportHttpService.class);
        httpServices.addBinding().to(UIRecipeHttpService.class);
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
        public void onDeleteProject(UIEvents.ProjectDeletedEvent event) {
            for (DashboardService.Dashboard dashboard : dashboardService.list(event.project)) {
                dashboardService.delete(event.project, dashboard.name);
            }
            if (customPageDatabase != null) {
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

                handle.createStatement("CREATE TABLE IF NOT EXISTS web_user_api_key (" +
                        "  id serial NOT NULL,\n" +
                        "  project_id INTEGER REFERENCES web_user_project(id),\n" +
                        "  scope_expression TEXT,\n" +
                        "  has_read_permission BOOLEAN NOT NULL,\n" +
                        "  user_id INT REFERENCES web_user(id)," +
                        "  has_write_permission BOOLEAN NOT NULL,\n" +
                        "  is_admin BOOLEAN DEFAULT false NOT NULL,\n" +
                        "  created_at timestamp DEFAULT now() NOT NULL\n" +
                        "  )")
                        .execute();

                handle.createStatement("CREATE TABLE IF NOT EXISTS web_user_project (\n" +
                        "  id serial NOT NULL,\n" +
                        "  project varchar(150) NOT NULL,\n" +
                        "  api_url varchar(250) NOT NULL,\n" +
                        "  created_at timestamp DEFAULT now() NOT NULL,\n" +
                        "  CONSTRAINT production UNIQUE(project, api_url),\n" +
                        "  PRIMARY KEY (id)\n" +
                        ")")
                        .execute();

                handle.createStatement("CREATE TABLE IF NOT EXISTS reports (" +
                        "  project_id INT REFERENCES web_user_project(id)," +
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
                        "  project_id INT REFERENCES web_user_project(id)," +
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
                        "  project_id INT REFERENCES web_user_project(id)," +
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

                if (config.getCustomPageBackend() == CustomPageBackend.JDBC) {
                    handle.createStatement("CREATE TABLE IF NOT EXISTS custom_page (" +
                            "  project_id INT REFERENCES web_user_project(id)," +
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

    public static class UIPermissionCheckProcessorProvider implements Provider<RequestPreProcessorItem> {

        private final DBI dbi;
        private final EncryptionConfig encryptionConfig;

        @Inject
        public UIPermissionCheckProcessorProvider(@Named("ui.metadata.jdbc") JDBCPoolDataSource dataSource, EncryptionConfig encryptionConfig) {
            dbi = new DBI(dataSource);
            this.encryptionConfig = encryptionConfig;
        }

        @Override
        public RequestPreProcessorItem get() {
            return new RequestPreProcessorItem(method -> method.getDeclaringClass().isAnnotationPresent(UIService.class), new RequestPreprocessor() {
                @Override
                public void handle(RakamHttpRequest request, ObjectNode jsonNodes) {
                    Integer userId = request.cookies().stream().filter(e -> e.name().equals("session"))
                            .findFirst()
                            .map(e -> extractUserFromCookie(e.value(), encryptionConfig.getSecretKey()))
                            .orElseThrow(() -> new RakamException(HttpResponseStatus.FORBIDDEN));

                    String projectId = request.headers().get("project");

                    if (projectId == null) {
                        new RakamException(HttpResponseStatus.FORBIDDEN);
                    }

                    try (Handle handle = dbi.open()) {
                        int id = Integer.parseInt(projectId);
                        boolean hasPermission = handle.createQuery("SELECT 1 FROM web_user_api_key key JOIN web_user_project project ON (key.project_id = project.id) WHERE key.user_id = :user AND project.id = :id")
                                .map(IntegerMapper.FIRST)
                                .bind("user", userId)
                                .bind("id", id)
                                .first() != null;
                        if (!hasPermission) {
                            new RakamException(HttpResponseStatus.FORBIDDEN);
                        }
                    }
                }
            });
        }
    }

    @Target({ElementType.TYPE, ElementType.METHOD})
    @Retention(RetentionPolicy.RUNTIME)
    public @interface UIService {
    }
}
