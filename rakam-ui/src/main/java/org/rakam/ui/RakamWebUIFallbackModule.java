package org.rakam.ui;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.multibindings.OptionalBinder;
import org.rakam.plugin.RakamModule;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.ui.page.CustomPageDatabase;
import org.rakam.util.IgnorePermissionCheck;

import javax.ws.rs.GET;
import javax.ws.rs.Path;

public class RakamWebUIFallbackModule extends RakamModule {
    @Override
    protected void setup(Binder binder) {
        OptionalBinder.newOptionalBinder(binder, CustomPageDatabase.class);

        if(!"true".equals(getConfig("ui.enable"))) {
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
        @Path("/*")
        @IgnorePermissionCheck
        public void main(RakamHttpRequest request) {

            request.response("Rakam API is successfully installed! \n---------- \n" +
                    "Visit api.rakam.io for API documentation.")
                    .end();
        }
    }
}
