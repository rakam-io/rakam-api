package org.rakam.ui.user.ldap;

import com.google.auto.service.AutoService;
import com.google.inject.Binder;
import com.google.inject.multibindings.OptionalBinder;
import org.rakam.plugin.RakamModule;
import org.rakam.ui.AuthService;
import org.rakam.util.ConditionalModule;

import static io.airlift.configuration.ConfigBinder.configBinder;

@AutoService(RakamModule.class)
@ConditionalModule(config = "ui.authentication", value = "ldap")
public class LdapModule
        extends RakamModule {
    @Override
    protected void setup(Binder binder) {
        OptionalBinder.newOptionalBinder(binder, AuthService.class).setBinding().to(LdapAuthService.class);
        configBinder(binder).bindConfig(LdapConfig.class);
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
