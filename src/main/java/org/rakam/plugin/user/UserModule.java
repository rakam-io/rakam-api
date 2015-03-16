package org.rakam.plugin.user;

import com.google.auto.service.AutoService;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import org.rakam.plugin.RakamModule;
import org.rakam.server.http.HttpService;

import static io.airlift.configuration.ConfigurationModule.bindConfig;

/**
 * Created by buremba <Burak Emre Kabakcı> on 14/03/15 16:17.
 */
@AutoService(RakamModule.class)
public class UserModule extends RakamModule {
    @Override
    protected void setup(Binder binder) {
        UserPluginConfig userPluginConfig = buildConfigObject(UserPluginConfig.class);
        binder.bind(UserStorage.class).to(userPluginConfig.getStorageClass());

        bindConfig(binder).to(UserPluginConfig.class);

        Multibinder<HttpService> eventMappers = Multibinder.newSetBinder(binder, HttpService.class);
        eventMappers.addBinding().to(ActorCollectorService.class).in(Scopes.SINGLETON);
    }

    @Override
    public String name() {
        return "Customer Analytics Module";
    }

    @Override
    public String description() {
        return "Eats your users";
    }
}
