package org.rakam.plugin.user;

import com.google.auto.service.AutoService;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import org.rakam.plugin.RakamModule;
import org.rakam.plugin.user.mailbox.UserMailboxHttpService;
import org.rakam.server.http.HttpService;

/**
 * Created by buremba <Burak Emre Kabakcı> on 14/03/15 16:17.
 */
@AutoService(RakamModule.class)
public class UserModule extends RakamModule {
    @Override
    protected void setup(Binder binder) {
        Multibinder<HttpService> httpServices = Multibinder.newSetBinder(binder, HttpService.class);
        httpServices.addBinding().to(UserHttpService.class).in(Scopes.SINGLETON);

        UserPluginConfig userPluginConfig = buildConfigObject(UserPluginConfig.class);

        if(userPluginConfig.isMailboxEnabled()) {
            httpServices.addBinding().to(UserMailboxHttpService.class).in(Scopes.SINGLETON);
        }
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
