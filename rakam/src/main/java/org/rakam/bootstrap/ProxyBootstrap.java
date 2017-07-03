package org.rakam.bootstrap;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Set;

public class ProxyBootstrap
        extends Bootstrap
{
    public ProxyBootstrap(Set<Module> modules)
    {
        super(modules);
    }

    @Override
    public Injector initialize()
            throws Exception
    {
        Field modules = Bootstrap.class.getDeclaredField("modules");
        modules.setAccessible(true);
        List<Module> installedModules = (List<Module>) modules.get(this);
        SystemRegistry systemRegistry = new SystemRegistry(null, ImmutableSet.copyOf(installedModules));

        modules.set(this, ImmutableList.builder().addAll(installedModules).add((Module) binder -> {
            binder.bind(SystemRegistry.class).toInstance(systemRegistry);
        }).build());

        Injector initialize = super.initialize();
        return initialize;
    }
}
