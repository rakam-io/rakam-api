package org.rakam.util;

import com.google.auto.service.AutoService;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import org.rakam.plugin.RakamModule;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;

import javax.ws.rs.GET;
import javax.ws.rs.Path;

@AutoService(RakamModule.class)
public class RakamHealthCheckModule extends RakamModule {
    @Override
    protected void setup(Binder binder) {
        Multibinder.newSetBinder(binder, HttpService.class).addBinding()
                .to(RootAPIInformationService.class).in(Scopes.SINGLETON);
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
                    "Visit app.rakam.io to register the API with Rakam Data Platform or api.rakam.io for API documentation.")
                    .end();
        }
    }
}