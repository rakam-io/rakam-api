package org.rakam.util.javascript;

import com.google.auto.service.AutoService;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.ConfigBinder;
import org.rakam.plugin.RakamModule;

@AutoService(RakamModule.class)
public class JavascriptModule
        extends RakamModule {
    @Override
    protected void setup(Binder binder) {
        ConfigBinder.configBinder(binder).bindConfig(JavascriptConfig.class);
        binder.bind(JSCodeCompiler.class).in(Scopes.SINGLETON);
    }

    @Override
    public String name() {
        return "Javascript executor module";
    }

    @Override
    public String description() {
        return null;
    }
}
