package org.rakam.ui;

import com.google.common.eventbus.Subscribe;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.binder.AnnotatedBindingBuilder;
import com.google.inject.multibindings.Multibinder;
import org.rakam.util.ConditionalModule;
import org.rakam.plugin.RakamModule;
import org.rakam.plugin.SystemEvents;
import org.rakam.server.http.HttpService;

import javax.inject.Inject;


@ConditionalModule(config = "ui.enable", value = "true")
public class RakamUIModule extends RakamModule {
    @Override
    protected void setup(Binder binder) {
        RakamUIConfig rakamUIConfig = buildConfigObject(RakamUIConfig.class);

        AnnotatedBindingBuilder<CustomPageDatabase> customPageDb = binder.bind(CustomPageDatabase.class);
        switch (rakamUIConfig.getCustomPageBackend()) {
            case FILE:
                customPageDb.to(FileBackedCustomPageDatabase.class).in(Scopes.SINGLETON);
                break;
            case JDBC:
                customPageDb.to(JDBCCustomPageDatabase.class).in(Scopes.SINGLETON);
                break;
        }

        binder.bind(DefaultDashboardCreator.class).asEagerSingleton();

        Multibinder<HttpService> httpServices = Multibinder.newSetBinder(binder, HttpService.class);
        httpServices.addBinding().to(ReportHttpService.class);
        httpServices.addBinding().to(CustomReportHttpService.class);
        httpServices.addBinding().to(CustomPageHttpService.class);
        httpServices.addBinding().to(DashboardService.class);
        httpServices.addBinding().to(WebUserHttpService.class);
        httpServices.addBinding().to(ProxyWebService.class);
        httpServices.addBinding().to(RakamUIWebService.class);
        httpServices.addBinding().to(UserUtilHttpService.class);
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
        public void onCreateProject(SystemEvents.ProjectCreatedEvent event) {
            service.create(event.project, "Default", null);
        }
    }
}
