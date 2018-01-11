package org.rakam.ui;

import com.google.auto.service.AutoService;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.multibindings.OptionalBinder;
import org.rakam.plugin.RakamModule;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.ui.customreport.CustomReportMetadata;
import org.rakam.ui.page.CustomPageDatabase;

import javax.ws.rs.GET;
import javax.ws.rs.Path;

@AutoService(RakamModule.class)
public class RakamWebUIFallbackModule extends RakamModule {
    @Override
    protected void setup(Binder binder) {
        OptionalBinder.newOptionalBinder(binder, CustomPageDatabase.class);
        OptionalBinder.newOptionalBinder(binder, CustomReportMetadata.class);
        OptionalBinder.newOptionalBinder(binder, ReportMetadata.class);
        OptionalBinder.newOptionalBinder(binder, DashboardService.class);

        if (!"true".equals(getConfig("ui.enable"))) {
            Multibinder.newSetBinder(binder, HttpService.class).addBinding()
                    .to(RootAPIInformationService.class).in(Scopes.SINGLETON);
        }
    }

    @Override
    public String name() {
        return "Fallback for Rakam API BI Module";
    }

    @Override
    public String description() {
        return null;
    }

    @Path("/")
    public static class RootAPIInformationService extends HttpService {
        @GET
        @Path("/")
        public void main(RakamHttpRequest request) {
            request.response("Rakam API is successfully installed! \n---------- \n" +
                    "Visit app.rakam.io to register the API with Rakam BI or api.rakam.io for API documentation.")
                    .end();
        }
    }
}
