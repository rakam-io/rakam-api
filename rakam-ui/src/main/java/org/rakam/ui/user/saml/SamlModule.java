package org.rakam.ui.user.saml;

import com.google.auto.service.AutoService;
import com.google.inject.Binder;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.multibindings.OptionalBinder;
import org.rakam.plugin.RakamModule;
import org.rakam.server.http.HttpService;
import org.rakam.ui.AuthService;
import org.rakam.util.ConditionalModule;

import static io.airlift.configuration.ConfigBinder.configBinder;

@AutoService(RakamModule.class)
@ConditionalModule(config = "ui.authentication", value = "saml")
public class SamlModule
        extends RakamModule {
    @Override
    protected void setup(Binder binder) {
        OptionalBinder.newOptionalBinder(binder, AuthService.class).setBinding().to(SamlAuthService.class);
        configBinder(binder).bindConfig(SamlConfig.class);

        Multibinder<HttpService> httpServices = Multibinder.newSetBinder(binder, HttpService.class);
        httpServices.addBinding().to(SamlHttpService.class);
    }

    @Override
    public String name() {
        return "Ldap Auth module";
    }

    @Override
    public String description() {
        return "Implements Ldap Auth Service";
    }
}
