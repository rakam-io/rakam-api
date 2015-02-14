package org.rakam.server.http;

import com.google.inject.Binder;
import org.rakam.plugin.RakamModule;

import static io.airlift.configuration.ConfigurationModule.bindConfig;

/**
 * Created by buremba <Burak Emre Kabakcı> on 14/02/15 22:54.
 */
public class HttpServerModule extends RakamModule {

    @Override
    protected void setup(Binder binder) {
        bindConfig(binder).prefixedWith("http.server").to(HttpServerConfig.class);
    }

    @Override
    public String name() {
        return null;
    }

    @Override
    public String description() {
        return null;
    }
}
