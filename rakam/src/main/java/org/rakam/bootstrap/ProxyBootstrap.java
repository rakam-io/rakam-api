package org.rakam.bootstrap;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.log.Logger;
import org.rakam.ServiceStarter;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public class ProxyBootstrap
        extends Bootstrap {
    private final static Logger LOGGER = Logger.get(ProxyBootstrap.class);

    public ProxyBootstrap(Set<Module> modules) {
        super(modules);
    }

    @Override
    public Injector initialize()
            throws Exception {
        Field modules = Bootstrap.class.getDeclaredField("modules");
        modules.setAccessible(true);
        List<Module> installedModules = (List<Module>) modules.get(this);
        SystemRegistry systemRegistry = new SystemRegistry(null, ImmutableSet.copyOf(installedModules));

        modules.set(this, ImmutableList.builder().addAll(installedModules).add((Module) binder -> {
            binder.bind(SystemRegistry.class).toInstance(systemRegistry);
        }).build());

        String env = System.getProperty("env");
        if (env != null) {
            LOGGER.info("Reading environment variables starting with `%s`", env);
            System.getenv().entrySet().stream()
                    .filter(entry -> entry.getKey().startsWith(env)).forEach(entry -> {
                String configName = entry.getKey().substring(env.length() + 1)
                        .toLowerCase(Locale.ENGLISH).replaceAll("_", ".");
                this.setOptionalConfigurationProperty(configName, entry.getValue());
            });
        }

        Injector initialize = super.initialize();
        return initialize;
    }
}
